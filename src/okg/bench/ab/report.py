"""Aggregation + money charts from run directories.

Reports are computed from stored per-task records ONLY. A run whose task
count is short of its manifest ordering is labeled ``partial`` — never
extrapolated. Charts (metric vs task index, one line per arm) render via
matplotlib under the ``[bench]`` extra with the standard loud-hint guard.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .errors import AbBenchError
from .manifest import load_manifest
from .model import dump_json, load_json

CHART_METRICS = (
    ("total_tokens", "tokens per task (in+out)"),
    ("tool_calls", "tool calls per task"),
    ("wall_s", "wall seconds per task"),
)


def load_run(run_dir: Path) -> dict[str, Any]:
    """One run's manifest + ordered task rows + scores."""
    manifest = load_manifest(run_dir)
    tasks_dir = run_dir / "tasks"
    rows: list[dict[str, Any]] = []
    if tasks_dir.exists():
        records = sorted(
            (load_json(p) for p in tasks_dir.glob("*.json")),
            key=lambda r: r["task_index"],
        )
        for rec in records:
            metrics = rec["result"]["metrics"]
            rows.append(
                {
                    "task_index": rec["task_index"],
                    "instance_id": rec["instance"]["instance_id"],
                    "status": rec["result"]["status"],
                    "input_tokens": metrics["input_tokens"],
                    "output_tokens": metrics["output_tokens"],
                    "total_tokens": metrics["input_tokens"] + metrics["output_tokens"],
                    "cache_read_tokens": metrics["cache_read_tokens"],
                    "tool_calls": metrics["tool_calls"],
                    "wall_s": metrics["wall_s"],
                    "between": rec.get("between", {}),
                }
            )
    scores_path = run_dir / "scores.json"
    scores = load_json(scores_path) if scores_path.exists() else []
    score_by_id = {s["instance_id"]: s for s in scores}
    for row in rows:
        s = score_by_id.get(row["instance_id"])
        # `score_recorded` distinguishes "the scorer never produced a record
        # for this instance" from "the scorer produced one and declined to
        # score it, with a reason". Both leave `score` None, and the paired
        # comparison must report the two differently — collapsing them is how
        # an unscored instance becomes an invisible one.
        row["score_recorded"] = s is not None
        row["score"] = None if s is None else s["value"]
        row["unscored_reason"] = None if s is None else s.get("unscored_reason")
        row["score_kind"] = None if s is None else s.get("kind")
    expected = len(manifest.ordering.get("instance_ids", []))
    return {
        "run_id": manifest.run_id,
        "manifest": manifest,
        "rows": rows,
        "expected_tasks": expected,
        "partial": len(rows) < expected,
    }


def summarize(run_dir: Path) -> dict[str, Any]:
    run = load_run(run_dir)
    rows = run["rows"]
    scored = [r for r in rows if r["score"] is not None]
    summary = {
        "run_id": run["run_id"],
        "benchmark": run["manifest"].benchmark,
        "arm": run["manifest"].arm,
        "model": run["manifest"].model,
        "partial": run["partial"],
        "tasks_recorded": len(rows),
        "tasks_expected": run["expected_tasks"],
        "completed": sum(1 for r in rows if r["status"] == "completed"),
        "errors": sum(1 for r in rows if r["status"] == "error"),
        "timeouts": sum(1 for r in rows if r["status"] == "timeout"),
        "total_tokens": sum(r["total_tokens"] for r in rows),
        # Timeout/error tasks whose result event was lost report 0 usage;
        # count them so a deflated token total is visibly labeled.
        "tasks_without_usage": sum(
            1
            for r in rows
            if r["status"] != "completed" and r["total_tokens"] == 0
        ),
        "total_tool_calls": sum(r["tool_calls"] for r in rows),
        "total_wall_s": round(sum(r["wall_s"] for r in rows), 1),
        "scored": len(scored),
        "unscored": len(rows) - len(scored),
        "mean_score": (
            round(sum(r["score"] for r in scored) / len(scored), 4) if scored else None
        ),
    }
    dump_json(run_dir / "summary.json", summary)
    return summary


def render_charts(
    run_dirs: list[Path], out_dir: Path
) -> list[Path]:
    """The money charts: metric vs task index, one line per run (arm).
    Multiple runs of the same benchmark overlay; mixing benchmarks refuses."""
    plt = _matplotlib()
    runs = [load_run(d) for d in run_dirs]
    benchmarks = {r["manifest"].benchmark for r in runs}
    if len(benchmarks) > 1:
        raise AbBenchError(
            f"refusing to chart across benchmarks: {sorted(benchmarks)}"
        )
    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for metric, label in CHART_METRICS:
        fig, ax = plt.subplots(figsize=(7, 4.2))
        for run in runs:
            rows = run["rows"]
            xs = [r["task_index"] for r in rows]
            ys = [r[metric] for r in rows]
            suffix = " (partial)" if run["partial"] else ""
            ax.plot(xs, ys, marker="o", markersize=3,
                    label=f"{run['manifest'].arm}{suffix}")
        ax.set_xlabel("task index (run order)")
        ax.set_ylabel(label)
        ax.set_title(f"{next(iter(benchmarks))}: {label} by arm")
        ax.legend()
        fig.tight_layout()
        path = out_dir / f"{metric}_by_task.png"
        fig.savefig(path, dpi=150)
        plt.close(fig)
        written.append(path)
    return written


def _matplotlib():
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise AbBenchError(
            "matplotlib is required for charts: install the bench extra "
            "(`uv sync --extra bench` / `pip install okg[bench]`)"
        ) from exc
    return plt
