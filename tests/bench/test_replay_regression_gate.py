from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

import okg.bench.orchestrator as orchestrator

Experiment = orchestrator.Experiment
Replay = orchestrator.Replay
Repo = orchestrator.Repo
build_lsp_benchmark_report = orchestrator.build_lsp_benchmark_report
build_replay_gate_report = orchestrator.build_replay_gate_report
build_replay_gate_junit = orchestrator.build_replay_gate_junit


def _exp() -> Experiment:
    return Experiment(
        id="demo-tier1-tail-3",
        repo=Repo(name="demo", url="https://example.invalid/demo.git"),
        source_set="tier1",
        replay=Replay(strategy="tail_window", last_n=3),
    )


def _lsp_exp() -> Experiment:
    return Experiment(
        id="demo-code-lsp-head10",
        repo=Repo(name="demo", url="https://example.invalid/demo.git"),
        source_set="files+code+lsp",
        replay=Replay(strategy="head_only", last_n=10),
    )


def _write_artifacts(
    tmp_path: Path,
    *,
    csv_rows: list[str],
    diagnostics: dict | None = None,
) -> Path:
    results = tmp_path / "results"
    results.mkdir()
    csv_path = results / "demo-tier1-tail-3.csv"
    csv_path.write_text(
        "\n".join([
            "i,commit,e2e_ms,wf_a0_ms,slowest_source,slowest_phase,slowest_phase_ms",
            *csv_rows,
        ])
        + "\n",
        encoding="utf-8",
    )
    if diagnostics is not None:
        (results / "demo-tier1-tail-3.diagnostics.json").write_text(
            json.dumps({
                "schema": "okg.bench.source_sync_diagnostics.v1",
                **diagnostics,
            }),
            encoding="utf-8",
        )
    return results


def _write_lsp_artifacts(
    tmp_path: Path,
    *,
    worker_log: str = "ok\n",
    replay_log: str = "ok\n",
) -> Path:
    results = tmp_path / "results"
    results.mkdir()
    (results / "demo-code-lsp-head10.csv").write_text(
        "\n".join([
            "i,commit,e2e_ms,gen_id",
            "1,aaa111,100,okg:demo:branch:default:gen:1",
        ])
        + "\n",
        encoding="utf-8",
    )
    (results / "demo-code-lsp-head10.worker.log").write_text(
        worker_log,
        encoding="utf-8",
    )
    (results / "demo-code-lsp-head10.replay.log").write_text(
        replay_log,
        encoding="utf-8",
    )
    return results


def _edge_counts() -> dict:
    return {
        "generation_id": "okg:demo:branch:default:gen:1",
        "final_nodes": 10,
        "final_edges": 20,
        "source": "demo-code-lsp-head10.code-lsp-resolution",
        "cross_file_references": 4,
        "cross_file_calls": 2,
        "cross_file_edges": 6,
        "total_source_edges": 6,
        "resolver_counts": {"pyright": 6},
    }


def _source_runs() -> dict:
    return {
        "source": "demo-code-lsp-head10.code-lsp-resolution",
        "run_count": 3,
        "active_run_count": 1,
        "emitted_run_count": 1,
        "statuses": {"resolved": 1, "no_change": 2},
        "failed_statuses": {},
        "no_change_request_violations": 0,
        "max_lsp_requests": 7,
        "max_files_opened": 2,
        "max_changed_files": 1,
        "total_edges_appended": 6,
        "total_edges_retracted": 0,
    }


def test_replay_gate_accepts_under_budget_artifacts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", _write_artifacts(
        tmp_path,
        csv_rows=[
            "1,aaa111,100,10,git-files,source_runner.adapter_run,40",
            "2,bbb222,200,20,git-files,source_runner.adapter_run,50",
            "3,ccc333,300,30,git-files,source_runner.adapter_run,60",
        ],
        diagnostics={
            "source_run_summary": {
                "sources": {"git-files": {"p95_ms": 50, "max_ms": 60}},
                "phases": {
                    "source_runner.adapter_run": {"p95_ms": 40, "max_ms": 50},
                },
            },
        },
    ))

    report = build_replay_gate_report(
        _exp(),
        max_p95_ms=500,
        max_max_ms=1000,
        min_throughput_commits_per_second=1.0,
        max_outlier_count=0,
        outlier_threshold_ms=1000,
        max_queue_dwell_p95_ms=100,
        require_diagnostics=True,
    )

    assert report["ok"] is True
    assert report["violation_count"] == 0
    assert report["metrics"]["latency"]["p95_ms"] == 300
    assert (
        report["metrics"]["owner_context"]["basis"]
        == "diagnostics_summary"
    )


def test_lsp_benchmark_report_requires_final_semantic_edges(
    tmp_path: Path,
    monkeypatch,
) -> None:
    exp = _lsp_exp()
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", _write_lsp_artifacts(tmp_path))
    monkeypatch.setattr(
        orchestrator,
        "_lsp_edge_counts_at_generation",
        lambda _exp, _generation_id: _edge_counts(),
    )
    monkeypatch.setattr(
        orchestrator,
        "_lsp_source_run_summary",
        lambda _exp: _source_runs(),
    )
    monkeypatch.setattr(
        orchestrator,
        "_lsp_pending_deferred_cursor",
        lambda _exp: {
            "source": "demo-code-lsp-head10.code-lsp-resolution",
            "pending": False,
            "paths": [],
            "estimated_lsp_requests": 0,
            "reason": None,
        },
    )

    report = build_lsp_benchmark_report(
        exp,
        min_references=1,
        min_calls=1,
        require_clean_logs=True,
    )

    assert report["ok"] is True
    assert report["edge_counts"]["cross_file_references"] == 4
    assert report["source_runs"]["emitted_run_count"] == 1
    assert report["generation_id"] == "okg:demo:branch:default:gen:1"


def test_lsp_benchmark_report_fails_on_source_failure_marker(
    tmp_path: Path,
    monkeypatch,
) -> None:
    exp = _lsp_exp()
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", _write_lsp_artifacts(
        tmp_path,
        worker_log="CodeParseFailed: python AST parse failed\n",
    ))
    monkeypatch.setattr(
        orchestrator,
        "_lsp_edge_counts_at_generation",
        lambda _exp, _generation_id: _edge_counts(),
    )
    monkeypatch.setattr(
        orchestrator,
        "_lsp_source_run_summary",
        lambda _exp: _source_runs(),
    )
    monkeypatch.setattr(
        orchestrator,
        "_lsp_pending_deferred_cursor",
        lambda _exp: {
            "source": "demo-code-lsp-head10.code-lsp-resolution",
            "pending": False,
            "paths": [],
            "estimated_lsp_requests": 0,
            "reason": None,
        },
    )

    report = build_lsp_benchmark_report(
        exp,
        min_references=1,
        min_calls=1,
        require_clean_logs=True,
    )

    assert report["ok"] is False
    assert report["violations"][0]["reason"] == "source_failure_markers_in_logs"
    assert report["violations"][0]["marker_count"] == 1


def test_replay_gate_reports_latency_outlier_and_owner_context(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", _write_artifacts(
        tmp_path,
        csv_rows=[
            "1,aaa111,100,10,git-files,source_runner.adapter_run,40",
            "2,bbb222,1300,20,code-structure,source_runner.parse,900",
            "3,ccc333,2500,30,code-structure,source_runner.parse,1400",
        ],
        diagnostics={
            "source_run_summary": {
                "sources": {
                    "git-files": {"p95_ms": 50, "max_ms": 60},
                    "code-structure": {"p95_ms": 1600, "max_ms": 2200},
                },
                "phases": {
                    "source_runner.parse": {"p95_ms": 1400, "max_ms": 1800},
                },
            },
        },
    ))

    report = build_replay_gate_report(
        _exp(),
        max_p95_ms=1000,
        max_max_ms=2000,
        min_throughput_commits_per_second=0.1,
        max_outlier_count=0,
        outlier_threshold_ms=1000,
        max_queue_dwell_p95_ms=None,
        require_diagnostics=True,
    )

    # Latency/outlier violations are ADVISORY (count-based-perf-gate): still
    # reported (with owner context), but non-blocking, so the gate passes.
    assert report["ok"] is True
    assert report["blocking_violation_count"] == 0
    reasons = {violation["reason"] for violation in report["violations"]}
    assert {
        "p95_budget_exceeded",
        "max_budget_exceeded",
        "outlier_count_exceeded",
    } <= reasons
    p95_violation = next(
        item for item in report["violations"]
        if item["reason"] == "p95_budget_exceeded"
    )
    assert p95_violation["owner_context"]["source"] == "code-structure"
    assert p95_violation["owner_context"]["phase"] == "source_runner.parse"


def test_replay_gate_owner_context_ignores_counter_phases(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", _write_artifacts(
        tmp_path,
        csv_rows=[
            "1,aaa111,1200,10,git-files,source_runner.adapter_run,100",
            "2,bbb222,1400,20,git-files,source_runner.adapter_run,120",
        ],
        diagnostics={
            "source_run_summary": {
                "sources": {"git-files": {"p95_ms": 120, "max_ms": 150}},
                "phases": {
                    "payload_bytes": {"p95_ms": 900000, "max_ms": 900000},
                    "source_runner.flush.chronos_apply.node_change_fields.name": {
                        "p95_ms": 800000,
                        "max_ms": 800000,
                    },
                    "source_runner.flush.chronos_apply.node_change_class.total": {
                        "p95_ms": 700000,
                        "max_ms": 700000,
                    },
                    "source_runner.adapter_run": {
                        "p95_ms": 120,
                        "max_ms": 150,
                    },
                },
            },
            "publish_run_summary": {
                "labels": {
                    "after-layer-1": {"p95_ms": 500, "max_ms": 600},
                },
                "stages": {
                    "native_views": {"p95_ms": 450, "max_ms": 550},
                    "rows_written": {"p95_ms": 100000, "max_ms": 100000},
                },
            },
        },
    ))

    report = build_replay_gate_report(
        _exp(),
        max_p95_ms=1000,
        max_max_ms=2000,
        min_throughput_commits_per_second=0.1,
        max_outlier_count=10,
        outlier_threshold_ms=1000,
        max_queue_dwell_p95_ms=None,
        require_diagnostics=True,
    )

    owner = report["metrics"]["owner_context"]
    assert owner["phase"] == "source_runner.adapter_run"
    assert owner["publish_stage"] == "native_views"
    assert owner["top_owner_kind"] == "publish_label"
    assert owner["top_owner"] == "after-layer-1"
    assert owner["top_owner_p95_ms"] == pytest.approx(500.0)


def test_replay_gate_classifies_queue_dwell_as_environmental(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", _write_artifacts(
        tmp_path,
        csv_rows=[
            "1,aaa111,100,50,git-files,source_runner.adapter_run,40",
            "2,bbb222,200,2000,git-files,source_runner.adapter_run,50",
            "3,ccc333,300,2500,git-files,source_runner.adapter_run,60",
        ],
    ))

    report = build_replay_gate_report(
        _exp(),
        max_p95_ms=1000,
        max_max_ms=2000,
        min_throughput_commits_per_second=1.0,
        max_outlier_count=0,
        outlier_threshold_ms=1000,
        max_queue_dwell_p95_ms=1000,
        require_diagnostics=False,
    )

    # queue_dwell is an environmental/wall-clock signal -> ADVISORY, non-blocking.
    assert report["ok"] is True
    assert report["blocking_violation_count"] == 0
    assert report["violations"] == [{
        "reason": "queue_dwell_p95_exceeded",
        "queue_dwell_p95_ms": 2500.0,
        "max_queue_dwell_p95_ms": 1000,
        "environment": True,
    }]


def test_replay_gate_reports_replay_timing_breakdown(
    tmp_path: Path,
    monkeypatch,
) -> None:
    results = tmp_path / "results"
    results.mkdir()
    (results / "demo-tier1-tail-3.csv").write_text(
        "\n".join([
            ",".join([
                "i",
                "commit",
                "e2e_ms",
                "wf_a0_ms",
                "source_total_ms",
                "ingest_source_wall_ms",
                "publish_window_total_ms",
                "bench_reset_to_workflow_created_ms",
                "bench_reset_to_workflow_started_ms",
                "bench_workflow_created_to_published_ms",
                "bench_workflow_started_to_published_ms",
                "bench_reset_ms",
                "bench_wait_new_gen_ms",
                "bench_latest_gen_query_ms",
                "bench_queue_drain_ms",
                "bench_gen_metrics_ms",
                "bench_db_size_ms",
                "slowest_source",
                "slowest_phase",
                "slowest_phase_ms",
            ]),
            (
                "1,aaa111,1000,10,200,150,300,250,260,700,690,"
                "5,950,40,70,1,3,git-files,source_runner,20"
            ),
            (
                "2,bbb222,1300,20,250,180,350,300,315,900,885,"
                "6,1200,80,90,2,4,code-structure,source_runner,30"
            ),
            (
                "3,ccc333,1500,30,300,220,400,350,370,1050,1030,"
                "7,1400,120,110,3,5,code-structure,source_runner,40"
            ),
        ])
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", results)

    report = build_replay_gate_report(
        _exp(),
        max_p95_ms=2000,
        max_max_ms=2500,
        min_throughput_commits_per_second=0.1,
        max_outlier_count=10,
        outlier_threshold_ms=1000,
        max_queue_dwell_p95_ms=None,
        require_diagnostics=False,
    )

    metrics = report["metrics"]
    assert report["ok"] is True
    assert metrics["producer_latency"]["p95_ms"] == pytest.approx(350)
    assert metrics["producer_start_latency"]["p95_ms"] == pytest.approx(370)
    assert (
        metrics["workflow_created_to_published"]["p95_ms"]
        == pytest.approx(1050)
    )
    assert metrics["source_total"]["p95_ms"] == pytest.approx(300)
    assert metrics["source_wall"]["p95_ms"] == pytest.approx(220)
    assert metrics["publish_window"]["p95_ms"] == pytest.approx(400)
    assert metrics["residual_unattributed"]["p95_ms"] == pytest.approx(800)
    assert metrics["residual_after_producer"]["p95_ms"] == pytest.approx(530)
    assert metrics["workflow_overhead"]["p95_ms"] == pytest.approx(430)
    assert metrics["published_latency"]["p95_ms"] == pytest.approx(1390)
    assert metrics["pre_workflow_to_published"]["p95_ms"] == pytest.approx(340)
    assert metrics["benchmark_queue_drain"]["p95_ms"] == pytest.approx(110)
    assert metrics["benchmark_latest_gen_query"]["mean_ms"] == pytest.approx(80)
    assert (
        metrics["owner_context"]["basis"]
        == "normalized_csv_timing_breakdown"
    )
    assert metrics["owner_context"]["top_owner_kind"] == "workflow_latency"
    assert (
        metrics["owner_context"]["top_owner"]
        == "workflow_created_to_published"
    )
    assert metrics["owner_context"]["top_component_owner_kind"] == (
        "workflow_overhead"
    )
    assert metrics["owner_context"]["top_component_owner"] == (
        "workflow_minus_source_minus_publish"
    )
    assert metrics["owner_context"]["top_component_owner_p95_ms"] == (
        pytest.approx(430)
    )


def test_replay_gate_cli_writes_report(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    results = _write_artifacts(
        tmp_path,
        csv_rows=[
            "1,aaa111,100,10,git-files,source_runner.adapter_run,40",
            "2,bbb222,200,20,git-files,source_runner.adapter_run,50",
        ],
    )
    output = tmp_path / "gate.json"
    exp = _exp()
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", results)
    monkeypatch.setattr(orchestrator, "_load_manifest", lambda: [exp])

    monkeypatch.setattr(sys, "argv", [
        "bench/orchestrator.py",
        "gate",
        exp.id,
        "--max-p95-ms",
        "500",
        "--max-max-ms",
        "1000",
        "--min-throughput-commits-per-second",
        "1",
        "--output",
        str(output),
        "--json",
    ])
    rc = orchestrator.main()

    assert rc == 0
    report = json.loads(capsys.readouterr().out)
    assert report["ok"] is True
    written = json.loads(output.read_text(encoding="utf-8"))
    assert written["schema"] == "okg.bench.sync_commit_replay_gate.v1"


# ---------------------------------------------------------------------------
# count-based-perf-gate / req.counts.gate
#
# A deterministic count-based junit case must PASS on the baseline and FAIL on
# an over-budget queries_per_changed_file, naming the offending commit and the
# observed-vs-budget counts. Wall-clock stays a separate advisory case.


def _write_count_artifacts(tmp_path: Path, *, csv_rows: list[str]) -> Path:
    results = tmp_path / "results"
    results.mkdir(parents=True)
    (results / "demo-tier1-tail-3.csv").write_text(
        "\n".join([
            "i,commit,e2e_ms,wf_a0_ms,files_changed,query_count,"
            "queries_per_changed_file,work_ratio,chronos_apply_ms,"
            "slowest_source,slowest_phase,slowest_phase_ms",
            *csv_rows,
        ])
        + "\n",
        encoding="utf-8",
    )
    return results


def _count_gate(exp, **overrides):
    kwargs = dict(
        max_p95_ms=10_000,
        max_max_ms=10_000,
        min_throughput_commits_per_second=0.0,
        max_outlier_count=10_000,
        outlier_threshold_ms=10_000,
        max_queue_dwell_p95_ms=None,
        require_diagnostics=False,
    )
    kwargs.update(overrides)
    return build_replay_gate_report(exp, **kwargs)


def test_count_gate_passes_on_baseline(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", _write_count_artifacts(
        tmp_path,
        csv_rows=[
            # 1-file resolving commit at the baseline query density.
            "1,aaa111,1200,10,1,158,158.0,0.04,26.0,"
            + "git-files,source_runner.flush.chronos_apply,12",
            # multi-file commit: same query_count spread over more files.
            "2,bbb222,1400,20,5,210,42.0,0.05,30.0,"
            + "git-files,source_runner.flush.chronos_apply,14",
        ],
    ))
    report = _count_gate(_exp(), max_queries_per_changed_file=200.0)
    assert report["ok"] is True
    assert report["violation_count"] == 0
    metrics = report["metrics"]["query_counts"]
    assert metrics["query_count_max"] == 210
    assert metrics["queries_per_changed_file"]["max_ms"] == pytest.approx(158.0)


def test_count_gate_fails_over_budget_with_commit_and_counts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", _write_count_artifacts(
        tmp_path,
        csv_rows=[
            "1,aaa111,1200,10,1,158,158.0,0.04,26.0,"
            + "git-files,source_runner.flush.chronos_apply,12",
            # orchestration-bound regression: 1 file, 451 round-trips.
            "2,deadbeef,1400,20,1,451,451.0,0.02,26.0,"
            + "git-files,source_runner.flush.chronos_apply,14",
        ],
    ))
    report = _count_gate(_exp(), max_queries_per_changed_file=200.0)
    assert report["ok"] is False
    count_violations = [
        v for v in report["violations"] if v.get("gate_class") == "count"
    ]
    assert len(count_violations) == 1
    v = count_violations[0]
    assert v["reason"] == "queries_per_changed_file_exceeded"
    assert v["commit"] == "deadbeef"
    assert v["queries_per_changed_file"] == pytest.approx(451.0)
    assert v["max_queries_per_changed_file"] == 200.0
    assert v["query_count"] == 451
    # The old hard-coded `deterministic: True` label claimed something the
    # gate never checked; it is gone.
    assert "deterministic" not in v


def test_count_gate_no_change_floor(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", _write_count_artifacts(
        tmp_path,
        csv_rows=[
            # lightest commit (0 files changed) but still 120 round-trips.
            "1,light000,1200,10,0,120,120.0,0.0,0.0,"
            + "git-files,source_runner.adapter_run,5",
            "2,bbb222,1400,20,5,210,42.0,0.05,30.0,"
            + "git-files,source_runner.flush.chronos_apply,14",
        ],
    ))
    report = _count_gate(
        _exp(),
        max_queries_per_changed_file=1000.0,  # not the trigger here
        max_no_change_query_count=50,
    )
    assert report["ok"] is False
    floor = [
        v for v in report["violations"]
        if v.get("reason") == "no_change_query_count_exceeded"
    ]
    assert len(floor) == 1
    assert floor[0]["commit"] == "light000"
    assert floor[0]["query_count"] == 120
    assert floor[0]["max_no_change_query_count"] == 50


def test_count_gate_junit_pass_and_fail(tmp_path: Path, monkeypatch) -> None:
    # PASS: baseline under budget → count testcase has no <failure>.
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", _write_count_artifacts(
        tmp_path / "pass",
        csv_rows=[
            "1,aaa111,1200,10,1,158,158.0,0.04,26.0,"
            "git-files,source_runner.flush.chronos_apply,12",
        ],
    ))
    pass_report = _count_gate(_exp(), max_queries_per_changed_file=200.0)
    pass_suite = build_replay_gate_junit(pass_report)
    cases = {c.get("name"): c for c in pass_suite.findall("testcase")}
    assert set(cases) == {
        "query_count_budget",
        "diagnostics_available",
        "wall_clock_latency_advisory",
    }
    assert cases["query_count_budget"].find("failure") is None

    # FAIL: over-budget commit → count testcase carries a <failure> naming it.
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", _write_count_artifacts(
        tmp_path / "fail",
        csv_rows=[
            "1,deadbeef,1400,20,1,451,451.0,0.02,26.0,"
            "git-files,source_runner.flush.chronos_apply,14",
        ],
    ))
    fail_report = _count_gate(_exp(), max_queries_per_changed_file=200.0)
    fail_suite = build_replay_gate_junit(fail_report)
    fail_cases = {c.get("name"): c for c in fail_suite.findall("testcase")}
    failure = fail_cases["query_count_budget"].find("failure")
    assert failure is not None
    assert "deadbeef" in failure.text
    assert "451" in failure.text
    assert "200" in failure.text
    # The advisory wall-clock case stays separate (no failure: budgets are huge).
    assert fail_cases["wall_clock_latency_advisory"].find("failure") is None
    assert fail_suite.get("failures") == "1"


# ---------------------------------------------------------------------------
# bench-count-gate-deterministic: per-commit baselines, missing and
# ambiguous data fail, pinned commit list.

_PAIRED_HEADER = (
    "i,commit,e2e_ms,wf_a0_ms,files_changed,query_count,"
    "queries_per_changed_file,wf_pairing_reason"
)


def _write_paired_rows(
    tmp_path: Path, rows: list[str], exp_id: str = "demo-tier1-tail-3",
) -> Path:
    results = tmp_path / "results"
    results.mkdir(parents=True)
    (results / f"{exp_id}.csv").write_text(
        "\n".join([_PAIRED_HEADER, *rows]) + "\n", encoding="utf-8",
    )
    return results


_SHA_A = "a" * 40
_SHA_B = "b" * 40


def _budget_exp(*, headroom: float = 1.10) -> Experiment:
    return Experiment(
        id="demo-tier1-tail-3",
        repo=Repo(name="demo", url="https://example.invalid/demo.git"),
        source_set="tier1",
        replay=Replay(strategy="head_only", last_n=2, commits=(_SHA_A, _SHA_B)),
        count_gate=orchestrator.CountGateBudget(
            headroom=headroom,
            baselines=((_SHA_A, 1000), (_SHA_B, 2000)),
        ),
    )


def _row(i: int, sha: str, queries: int | str, reason: str = "time_window",
         files: int | str = 2) -> str:
    qpcf = "" if queries == "" else f"{int(queries) / max(1, int(files or 0)):.3f}"
    return f"{i},{sha[:10]},1000,10,{files},{queries},{qpcf},{reason}"


def _count_reasons(report: dict) -> list[str]:
    return [
        v["reason"] for v in report["violations"]
        if v.get("gate_class") == "count"
    ]


def test_count_gate_passes_within_headroom(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", _write_paired_rows(
        tmp_path, [_row(1, _SHA_A, 1100), _row(2, _SHA_B, 1990)],
    ))
    report = _count_gate(_budget_exp())
    assert _count_reasons(report) == []
    assert report["ok"] is True
    per_commit = report["metrics"]["query_counts"]["per_commit"]
    assert [c["baseline_query_count"] for c in per_commit] == [1000, 2000]
    assert per_commit[0]["ratio_to_baseline"] == pytest.approx(1.1)
    assert report["budgets"]["headroom"] == 1.10


def test_count_gate_fails_commit_over_its_baseline(
    tmp_path: Path, monkeypatch,
) -> None:
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", _write_paired_rows(
        tmp_path, [_row(1, _SHA_A, 1000), _row(2, _SHA_B, 2201)],
    ))
    report = _count_gate(_budget_exp())
    assert report["ok"] is False
    violations = [
        v for v in report["violations"]
        if v["reason"] == "query_count_over_baseline"
    ]
    assert [v["commit"] for v in violations] == [_SHA_B[:10]]
    assert violations[0]["baseline_query_count"] == 2000
    assert violations[0]["max_query_count"] == pytest.approx(2200.0)
    message = orchestrator._count_violation_message(violations[0])
    assert "2201" in message and "2000" in message and "1.1" in message
    failure = build_replay_gate_junit(report).find(
        "testcase[@name='query_count_budget']/failure"
    )
    assert failure is not None and _SHA_B[:10] in failure.text


def test_count_gate_fails_on_commit_without_paired_workflow(
    tmp_path: Path, monkeypatch,
) -> None:
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", _write_paired_rows(
        tmp_path,
        [
            _row(1, _SHA_A, 1000),
            # No workflow landed in the replay window: query_count is empty.
            _row(2, _SHA_B, "", "workflow_not_in_replay_window"),
        ],
    ))
    report = _count_gate(_budget_exp())
    assert report["ok"] is False
    missing = [
        v for v in report["violations"]
        if v.get("reason") == "query_count_missing"
    ]
    assert [v["commit"] for v in missing] == [_SHA_B[:10]]
    assert "workflow_not_in_replay_window" in missing[0]["detail"]
    assert report["metrics"]["query_counts"]["unmeasured_commit_count"] == 1
    failure = build_replay_gate_junit(report).find(
        "testcase[@name='query_count_budget']/failure"
    )
    assert failure is not None
    assert "not measured" in failure.text


def test_count_gate_fails_on_ambiguous_workflow_window(
    tmp_path: Path, monkeypatch,
) -> None:
    # Two successful source-sync workflows in one window: the paired count is
    # only part of the commit's work, so the gate refuses it even though it
    # is under budget.
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", _write_paired_rows(
        tmp_path,
        [_row(1, _SHA_A, 900, "ambiguous_time_window"), _row(2, _SHA_B, 2000)],
    ))
    report = _count_gate(_budget_exp())
    assert report["ok"] is False
    assert _count_reasons(report) == ["query_count_ambiguous"]


def test_count_gate_fails_when_no_rows_measured(
    tmp_path: Path, monkeypatch,
) -> None:
    monkeypatch.setattr(
        orchestrator, "RESULTS_DIR", _write_paired_rows(tmp_path, []),
    )
    report = _count_gate(_budget_exp())
    assert report["ok"] is False
    assert "query_count_missing" in _count_reasons(report)


def test_count_gate_per_commit_budget_ignores_files_changed(
    tmp_path: Path, monkeypatch,
) -> None:
    # The per-commit budget never divides by files_changed, so an empty
    # files_changed neither passes nor fails a commit by itself.
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", _write_paired_rows(
        tmp_path,
        [_row(1, _SHA_A, 1000, files=""), _row(2, _SHA_B, 2000, files=0)],
    ))
    assert _count_reasons(_count_gate(_budget_exp())) == []


def test_count_gate_fails_on_pinned_commit_drift(
    tmp_path: Path, monkeypatch,
) -> None:
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", _write_paired_rows(
        tmp_path, [_row(1, _SHA_A, 1000), _row(2, "c" * 40, 2000)],
    ))
    report = _count_gate(_budget_exp())
    assert report["ok"] is False
    assert sorted(_count_reasons(report)) == [
        "baseline_missing", "replayed_commits_mismatch",
    ]


def test_count_gate_inactive_without_any_count_budget(
    tmp_path: Path, monkeypatch,
) -> None:
    # No manifest budget and no flag: the count gate is not configured, so it
    # reports nothing rather than inventing a budget.
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", _write_paired_rows(
        tmp_path, [_row(1, _SHA_A, "", "workflow_not_in_replay_window")],
    ))
    report = _count_gate(_exp())
    assert _count_reasons(report) == []


def test_baselines_and_headroom_go_together() -> None:
    with pytest.raises(ValueError, match="go together"):
        orchestrator._count_gate_violations(
            {"per_commit": []},
            max_queries_per_changed_file=None,
            max_no_change_query_count=None,
            baselines=((_SHA_A, 1),),
        )


def _manifest_entry(**overrides) -> dict:
    entry = {
        "id": "demo-head2",
        "repo": {"name": "demo", "url": "https://example.invalid/demo.git"},
        "source_set": "files+code",
        "replay": {
            "strategy": "head_only",
            "last_n": 2,
            "commits": [_SHA_A, _SHA_B],
        },
        "count_gate": {
            "headroom": 1.1,
            "baselines": {_SHA_A: 1000, _SHA_B: 2000},
        },
    }
    entry.update(overrides)
    return entry


def test_manifest_parses_pinned_commits_and_count_gate() -> None:
    exp = orchestrator._parse_experiment(_manifest_entry())
    assert exp.replay.commits == (_SHA_A, _SHA_B)
    assert exp.count_gate == orchestrator.CountGateBudget(
        headroom=1.1, baselines=((_SHA_A, 1000), (_SHA_B, 2000)),
    )


@pytest.mark.parametrize(
    ("override", "match"),
    [
        ({"replay": {"strategy": "head_only", "last_n": 3,
                     "commits": [_SHA_A, _SHA_B]}}, "must equal"),
        ({"replay": {"strategy": "head_only", "last_n": 1,
                     "commits": ["abc123"]}}, "full 40-hex"),
        ({"replay": {"strategy": "first_n", "n": 1,
                     "commits": [_SHA_A]}}, "only supported"),
        ({"replay": {"strategy": "head_only", "last_n": 2,
                     "commits": [_SHA_A, _SHA_A]}}, "duplicate"),
        ({"replay": {"strategy": "head_only", "last_n": 2}},
         "needs a pinned"),
        ({"count_gate": {"headroom": 1.1}}, "missing"),
        ({"count_gate": {"headroom": 1.0,
                         "baselines": {_SHA_A: 1, _SHA_B: 1}}}, "above 1.0"),
        ({"count_gate": {"headroom": 1.1, "baselines": {_SHA_A: 1}}},
         "exactly the pinned"),
        ({"count_gate": {"headroom": 1.1,
                         "baselines": {_SHA_A: 0, _SHA_B: 1}}},
         "positive integers"),
        ({"count_gate": {"headroom": 1.1,
                         "baselines": {_SHA_A: 1, _SHA_B: 1},
                         "extra": 1}}, "unknown"),
    ],
)
def test_manifest_rejects_bad_pin_or_budget(override: dict, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        orchestrator._parse_experiment(_manifest_entry(**override))


# Measured on lab-host, 2026-10-01: the highest query_count per pinned commit
# across runs pin-a, pin-b, final-c. (commit, files_changed, query_count)
_TINYDB_HEAD10_MEASURED = [
    ("e70f9b1d91", 2, 3996),
    ("770486ff82", 3, 3568),
    ("1e39ad3a2d", 4, 1153),
    ("9afda0c57b", 1, 1121),
    ("8d3f0c7c9c", 3, 2973),
    ("f16aec4fed", 3, 1387),
    ("4aa53111d7", 2, 3361),
    ("19066e0313", 2, 2518),
    ("bd034d0e95", 2, 4502),
    ("18d73a1506", 2, 2549),
]


def _tinydb_head10(monkeypatch) -> Experiment:
    monkeypatch.setattr(
        orchestrator, "MANIFEST",
        Path(__file__).resolve().parents[2] / "bench" / "manifest.yaml",
    )
    return orchestrator._resolve("tinydb-code-lsp-head10")


def _measured_rows(scale_commit: str | None = None, factor: float = 1.0):
    rows = []
    for i, (commit, files, queries) in enumerate(_TINYDB_HEAD10_MEASURED, 1):
        if commit == scale_commit:
            queries = int(queries * factor)
        rows.append(_row(i, commit, queries, files=files))
    return rows


def test_tinydb_head10_manifest_is_pinned_and_refreshes_every_publish(
    monkeypatch,
) -> None:
    exp = _tinydb_head10(monkeypatch)
    assert exp.replay.commits is not None
    assert len(exp.replay.commits) == exp.replay.last_n == 10
    assert exp.replay.commits[-1].startswith("18d73a1506")
    # A wall-clock-gated view refresh lands on whichever commit crosses the
    # interval, which made same-commit counts disagree between runs.
    assert exp.config.native_view_min_refresh_interval_ms == 0
    assert exp.config.native_view_materialization_delay_ms == 0
    assert exp.count_gate is not None
    assert exp.count_gate.headroom == pytest.approx(1.10)
    assert [
        (sha[:10], value) for sha, value in exp.count_gate.baselines
    ] == [(c, q) for c, _f, q in _TINYDB_HEAD10_MEASURED]


def test_tinydb_head10_budget_passes_the_measured_runs(
    tmp_path: Path, monkeypatch,
) -> None:
    exp = _tinydb_head10(monkeypatch)
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", _write_paired_rows(
        tmp_path, _measured_rows(), exp.id,
    ))
    report = _count_gate(exp)
    assert _count_reasons(report) == []
    assert report["metrics"]["query_counts"]["ratio_to_baseline_max"] == 1.0


@pytest.mark.parametrize(
    "commit", [commit for commit, _files, _q in _TINYDB_HEAD10_MEASURED],
)
def test_tinydb_head10_budget_fails_a_1_2x_regression_on_any_commit(
    tmp_path: Path, monkeypatch, commit: str,
) -> None:
    exp = _tinydb_head10(monkeypatch)
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", _write_paired_rows(
        tmp_path, _measured_rows(commit, 1.2), exp.id,
    ))
    report = _count_gate(exp)
    violations = [
        v for v in report["violations"] if v.get("gate_class") == "count"
    ]
    assert [(v["reason"], v["commit"]) for v in violations] == [
        ("query_count_over_baseline", commit),
    ]


# --- pinning helpers (L4) ---------------------------------------------------


def _git(repo: Path, *args: str) -> str:
    import subprocess

    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True, capture_output=True, text=True,
        env={
            "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.invalid",
            "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.invalid",
            "PATH": __import__("os").environ["PATH"], "HOME": str(repo),
        },
    ).stdout.strip()


def _linear_repo(tmp_path: Path, n: int) -> tuple[Path, list[str]]:
    repos = tmp_path / "repos"
    repo = repos / "demo"
    repo.mkdir(parents=True)
    _git(repo, "init", "-q")
    shas = []
    for i in range(n):
        (repo / "f.txt").write_text(f"{i}\n", encoding="utf-8")
        _git(repo, "add", "f.txt")
        _git(repo, "commit", "-q", "-m", f"c{i}")
        shas.append(_git(repo, "rev-parse", "HEAD"))
    return repos, shas


def _pinned_exp(commits: tuple[str, ...]) -> Experiment:
    return Experiment(
        id="demo-pinned",
        repo=Repo(name="demo", url="https://example.invalid/demo.git"),
        source_set="files",
        replay=Replay(strategy="head_only", last_n=len(commits), commits=commits),
    )


def test_pinned_reset_baseline_and_commit_list(tmp_path: Path, monkeypatch) -> None:
    repos, shas = _linear_repo(tmp_path, 6)
    monkeypatch.setattr(orchestrator, "BENCH_REPOS_DIR", repos)
    exp = _pinned_exp(tuple(shas[2:5]))  # pin c2..c4; c5 is "upstream HEAD"
    assert _git(repos / "demo", "rev-parse", "HEAD") == shas[5]

    orchestrator._reset_to_pinned_head(exp)
    assert _git(repos / "demo", "rev-parse", "HEAD") == shas[4]
    assert orchestrator._commits_for(exp) == shas[2:5]
    # Baseline is the first pinned commit's parent wherever HEAD points.
    _git(repos / "demo", "reset", "-q", "--hard", shas[0])
    assert orchestrator._baseline_commit(exp) == shas[1]


def test_commits_for_refuses_drift_from_the_pin(
    tmp_path: Path, monkeypatch,
) -> None:
    repos, shas = _linear_repo(tmp_path, 6)
    monkeypatch.setattr(orchestrator, "BENCH_REPOS_DIR", repos)
    # HEAD at c5 (upstream moved on) without the pinned reset: the computed
    # last-3 list is c3..c5, not the pinned c2..c4.
    exp = _pinned_exp(tuple(shas[2:5]))
    with pytest.raises(RuntimeError, match="do not match the pinned"):
        orchestrator._commits_for(exp)


def test_reset_to_pinned_head_fails_on_unknown_sha(
    tmp_path: Path, monkeypatch,
) -> None:
    import subprocess

    repos, _shas = _linear_repo(tmp_path, 2)
    monkeypatch.setattr(orchestrator, "BENCH_REPOS_DIR", repos)
    with pytest.raises(subprocess.CalledProcessError):
        orchestrator._reset_to_pinned_head(_pinned_exp(("d" * 40,)))


def test_reset_to_pinned_head_is_a_no_op_without_a_pin(
    tmp_path: Path, monkeypatch,
) -> None:
    repos, shas = _linear_repo(tmp_path, 2)
    monkeypatch.setattr(orchestrator, "BENCH_REPOS_DIR", repos)
    orchestrator._reset_to_pinned_head(_exp())
    assert _git(repos / "demo", "rev-parse", "HEAD") == shas[1]
