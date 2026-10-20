"""req.abx.metrics: aggregation, partial labeling, money charts."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.bench.ab.errors import AbBenchError
from okg.bench.ab.report import load_run, render_charts, summarize


def _make_run(
    runs_root: Path, run_id: str, arm: str, n_tasks: int, n_expected: int,
    benchmark: str = "swebench-verified",
) -> Path:
    run_dir = runs_root / run_id
    (run_dir / "tasks").mkdir(parents=True)
    manifest = {
        "schema_version": "okg-bench-ab.v1",
        "run_id": run_id,
        "created_at": "2026-07-27T00:00:00Z",
        "benchmark": benchmark,
        "arm": arm,
        "model": "sonnet",
        "claude_cli_version": "2.1.195 (Claude Code)",
        "framework_git_sha": None,
        "dataset_ref": {"dataset": "d", "revision": "r"},
        "subset": None,
        "pilot": True,
        "limit": None,
        "prompt_digest": "p",
        "ordering": {"mode": "pilot",
                     "instance_ids": [f"i{k}" for k in range(n_expected)]},
        "budgets": {"task_timeout_s": 1800.0},
        "native_memory_mode": None,
        "judge": None,
        "corpus": None,
        "argv": [],
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest))
    scores = []
    for k in range(n_tasks):
        record = {
            "task_index": k,
            "instance": {"instance_id": f"i{k}"},
            "result": {
                "instance_id": f"i{k}", "arm": arm, "status": "completed",
                "answer": "", "patch": None, "session_id": None, "error": None,
                "detail": {},
                "metrics": {
                    "input_tokens": 1000 - 50 * k, "output_tokens": 100,
                    "cache_read_tokens": 0, "cache_creation_tokens": 0,
                    "tool_calls": 10 - k, "wall_s": 30.0, "num_turns": 5,
                    "cost_usd": None,
                },
            },
            "between": {},
            "recorded_at": "2026-07-27T00:00:00Z",
        }
        (run_dir / "tasks" / f"i{k}.json").write_text(json.dumps(record))
        scores.append({"instance_id": f"i{k}", "value": 1.0 if k % 2 == 0 else 0.0,
                       "kind": "resolve", "detail": {}, "unscored_reason": None})
    (run_dir / "scores.json").write_text(json.dumps(scores))
    return run_dir


def test_summarize_complete_run(tmp_path):
    run_dir = _make_run(tmp_path, "r-full", "fresh", 4, 4)
    summary = summarize(run_dir)
    assert summary["partial"] is False
    assert summary["tasks_recorded"] == 4
    assert summary["total_tokens"] == sum((1000 - 50 * k) + 100 for k in range(4))
    assert summary["mean_score"] == 0.5
    assert (run_dir / "summary.json").exists()


def test_partial_run_is_labeled_not_extrapolated(tmp_path):
    run_dir = _make_run(tmp_path, "r-part", "okg", 2, 5)
    summary = summarize(run_dir)
    assert summary["partial"] is True
    assert summary["tasks_recorded"] == 2
    assert summary["tasks_expected"] == 5


def test_charts_render_one_line_per_arm(tmp_path):
    pytest.importorskip("matplotlib")
    dirs = [
        _make_run(tmp_path, "r-fresh", "fresh", 3, 3),
        _make_run(tmp_path, "r-okg", "okg", 3, 3),
    ]
    written = render_charts(dirs, tmp_path / "charts")
    names = {p.name for p in written}
    assert names == {"total_tokens_by_task.png", "tool_calls_by_task.png",
                     "wall_s_by_task.png"}
    for p in written:
        assert p.stat().st_size > 0


def test_charts_refuse_cross_benchmark_mixing(tmp_path):
    pytest.importorskip("matplotlib")
    dirs = [
        _make_run(tmp_path, "r-a", "fresh", 1, 1, benchmark="swebench-verified"),
        _make_run(tmp_path, "r-b", "fresh", 1, 1, benchmark="swe-qa"),
    ]
    with pytest.raises(AbBenchError, match="across benchmarks"):
        render_charts(dirs, tmp_path / "charts")


def test_load_run_joins_scores(tmp_path):
    run_dir = _make_run(tmp_path, "r-j", "native", 2, 2)
    run = load_run(run_dir)
    assert run["rows"][0]["score"] == 1.0
    assert run["rows"][1]["score"] == 0.0
