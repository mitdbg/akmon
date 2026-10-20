#!/usr/bin/env python3
"""Plot suite for the commit-replay bench.

Emits publication-quality PDFs under bench/plots/. Two flavors:

  per_experiment/<id>__grid.pdf      ONE 2x3-panel summary PDF per
                                      experiment (every signal in one
                                      place; meant to be the artifact
                                      you actually look at)
  cross/cross_*.pdf                   comparison plots across experiments

Usage:
  python bench/plot.py <experiment_id>     # per-experiment only
  python bench/plot.py --all               # all per-experiment + cross
  python bench/plot.py --cross             # cross-cutting only
"""
from __future__ import annotations

import csv
import contextlib
import math
import os
import sys
from pathlib import Path
from typing import Optional

try:
    import matplotlib.pyplot as plt
    import numpy as np
except ImportError as _plot_exc:  # pragma: no cover - wheel test
    raise ImportError(
        "okg.bench.plot needs matplotlib — install the bench extra: "
        "pip install 'okg[bench]'"
    ) from _plot_exc

BENCH = Path(os.environ.get("OKG_BENCH_ROOT", Path.cwd() / "bench")).resolve()
ROOT = BENCH.parent
RESULTS = BENCH / "results"
PLOTS_PER = BENCH / "plots" / "per_experiment"
PLOTS_CROSS = BENCH / "plots" / "cross"

from okg.bench.orchestrator import _load_manifest  # noqa: E402
from okg.bench.style import (  # noqa: E402
    apply_paper_style, color_for_repo, label_for_source_set,
    linestyle_for_source_set,
)


# ---------------------------------------------------------------------------
# Data loading

def load_csv(experiment_id: str) -> list[dict]:
    path = RESULTS / f"{experiment_id}.csv"
    if not path.exists():
        return []
    with path.open() as fh:
        return [_coerce(r) for r in csv.DictReader(fh)]


def _coerce(r: dict) -> dict:
    out = dict(r)
    for k in (
        "i", "files_changed", "lines_added", "lines_deleted",
        "e2e_ms", "cycle_ms", "nodes_added", "edges_added",
        "nodes_live", "edges_live", "gen_id", "db_size_bytes",
        "wf_a0_ms", "wf_a1_ms", "wf_setup_ms", "wf_ingest_ms",
        "wf_close_ms",
    ):
        v = out.get(k, "")
        if v in ("", "NaN", None):
            out[k] = float("nan")
        else:
            try:
                out[k] = int(v)
            except ValueError:
                with contextlib.suppress(ValueError):
                    out[k] = float(v)
    return out


def _i_array(rows: list[dict]) -> np.ndarray:
    return np.array([r["i"] for r in rows], dtype=float)


def _arr(rows: list[dict], key: str) -> np.ndarray:
    return np.array([r[key] for r in rows], dtype=float)


def _pctile(rows: list[dict], key: str, q: float) -> float:
    vs = [r[key] for r in rows if not math.isnan(r[key])]
    if not vs:
        return float("nan")
    return float(np.percentile(vs, q))


# ---------------------------------------------------------------------------
# Per-experiment plot panels (composed into a grid by per_experiment).

def _draw_latency_timeline(ax, exp_id, rows, color):
    x = _i_array(rows)
    y = _arr(rows, "e2e_ms")
    ax.plot(x, y, "o", color=color, markersize=2.0, alpha=0.7,
            label="per-commit")
    p50 = _pctile(rows, "e2e_ms", 50)
    p95 = _pctile(rows, "e2e_ms", 95)
    p99 = _pctile(rows, "e2e_ms", 99)
    if not math.isnan(p50):
        ax.axhline(p50, color="gray", linestyle="--", linewidth=0.8,
                   label=f"p50 = {p50:.0f} ms")
    if not math.isnan(p95):
        ax.axhline(p95, color="gray", linestyle=":", linewidth=0.8,
                   label=f"p95 = {p95:.0f} ms")
    ax.set_xlabel("commit index (chronological)")
    ax.set_ylabel("e2e latency (ms)")
    ax.set_title(
        f"per-commit latency  (p50/p95/p99 = "
        f"{p50:.0f}/{p95:.0f}/{p99:.0f} ms)"
    )
    ax.legend(loc="upper left", frameon=True, fontsize=7)


def _draw_latency_cdf(ax, rows, color):
    y = _arr(rows, "e2e_ms")
    y = y[~np.isnan(y)]
    if len(y) == 0:
        ax.text(0.5, 0.5, "(no data)", ha="center", va="center",
                transform=ax.transAxes)
        return
    ys = np.sort(y)
    cdf = np.arange(1, len(ys) + 1) / len(ys)
    ax.plot(ys, cdf, color=color, linewidth=1.5)
    # Reference percentile lines
    for q, ls in [(50, "--"), (95, ":")]:
        v = float(np.percentile(ys, q))
        ax.axvline(v, color="gray", linestyle=ls, linewidth=0.6, alpha=0.6)
    ax.set_xlabel("e2e latency (ms)")
    ax.set_ylabel("empirical CDF")
    ax.set_title("latency CDF")
    ax.set_ylim(0, 1.02)


def _draw_latency_hist(ax, rows, color):
    y = _arr(rows, "e2e_ms")
    y = y[~np.isnan(y)]
    if len(y) == 0:
        ax.text(0.5, 0.5, "(no data)", ha="center", va="center",
                transform=ax.transAxes)
        return
    n_bins = min(30, max(8, int(np.sqrt(len(y)))))
    ax.hist(y, bins=n_bins, color=color, alpha=0.8,
            edgecolor="black", linewidth=0.4)
    ax.set_xlabel("e2e latency (ms)")
    ax.set_ylabel("count")
    ax.set_title("latency histogram")


def _draw_graph_growth(ax, rows, color_n):
    """Two-line plot of nodes_live + edges_live over commit index.

    Hides any series that is identically zero/missing so we don't get
    degenerate ±0.04 right-axis artifacts. If both series are present
    and varying, uses a twin axis. If only one varies, single axis.
    """
    color_e = "#666666"
    x = _i_array(rows)
    nodes = _arr(rows, "nodes_live")
    edges = _arr(rows, "edges_live")
    nodes_finite = nodes[~np.isnan(nodes)]
    edges_finite = edges[~np.isnan(edges)]
    nodes_varies = (
        len(nodes_finite) > 0 and float(np.ptp(nodes_finite)) > 0
    )
    edges_varies = (
        len(edges_finite) > 0 and float(np.ptp(edges_finite)) > 0
    )
    if nodes_varies and edges_varies:
        ax.plot(x, nodes, color=color_n, linewidth=1.4, label="nodes_live")
        ax.set_ylabel("nodes_live", color=color_n)
        ax.tick_params(axis="y", labelcolor=color_n)
        ax2 = ax.twinx()
        ax2.plot(x, edges, color=color_e, linewidth=1.0,
                 linestyle="--", label="edges_live")
        ax2.set_ylabel("edges_live", color=color_e)
        ax2.tick_params(axis="y", labelcolor=color_e)
        h1, l1 = ax.get_legend_handles_labels()
        h2, l2 = ax2.get_legend_handles_labels()
        ax.legend(h1 + h2, l1 + l2, loc="upper left", frameon=True, fontsize=7)
    elif nodes_varies:
        ax.plot(x, nodes, color=color_n, linewidth=1.4, label="nodes_live")
        ax.set_ylabel("nodes_live")
        # Annotate that edges are zero/missing so the reader doesn't
        # wonder where the dashed line went.
        ax.text(0.98, 0.05, "edges_live == 0 (no edges in this config)",
                ha="right", va="bottom", transform=ax.transAxes,
                fontsize=7, color="gray")
        ax.legend(loc="upper left", frameon=True, fontsize=7)
    elif edges_varies:
        ax.plot(x, edges, color=color_e, linewidth=1.4,
                linestyle="--", label="edges_live")
        ax.set_ylabel("edges_live")
        ax.text(0.98, 0.05,
                f"nodes_live == "
                f"{int(nodes_finite[0]) if len(nodes_finite) else 0} "
                "(constant)",
                ha="right", va="bottom", transform=ax.transAxes,
                fontsize=7, color="gray")
        ax.legend(loc="upper left", frameon=True, fontsize=7)
    else:
        ax.text(0.5, 0.5, "(no growth — both series flat)",
                ha="center", va="center", transform=ax.transAxes,
                fontsize=9, color="gray")
    ax.set_xlabel("commit index (chronological)")
    ax.set_title("graph growth")


def _draw_cycle_vs_graphsize(ax, rows, color):
    """Scatter of publisher cycle_ms vs whichever graph-size axis
    actually varies (nodes_live xor edges_live xor both)."""
    nodes = _arr(rows, "nodes_live")
    edges = _arr(rows, "edges_live")
    cyc = _arr(rows, "cycle_ms")
    nodes_finite = nodes[~np.isnan(nodes)]
    edges_finite = edges[~np.isnan(edges)]
    nodes_varies = (
        len(nodes_finite) > 0 and float(np.ptp(nodes_finite)) > 0
    )
    edges_varies = (
        len(edges_finite) > 0 and float(np.ptp(edges_finite)) > 0
    )
    if nodes_varies:
        x_axis = nodes
        x_label = "nodes_live (graph size at this commit)"
    elif edges_varies:
        x_axis = edges
        x_label = "edges_live (graph size at this commit)"
    else:
        ax.text(0.5, 0.5, "(graph size flat — no variation to plot)",
                ha="center", va="center", transform=ax.transAxes,
                fontsize=9, color="gray")
        ax.set_title("publisher cycle vs graph size")
        return
    mask = ~np.isnan(x_axis) & ~np.isnan(cyc)
    ax.scatter(x_axis[mask], cyc[mask], color=color,
               alpha=0.5, s=8, edgecolors="none")
    ax.set_xlabel(x_label)
    ax.set_ylabel("publisher cycle_ms")
    ax.set_title("does publisher cost scale with graph size?")


def _draw_phase_breakdown(ax, rows):
    """Stacked-area of the source_sync workflow's NON-OVERLAPPING phases.

    Layout (bottom-up):
        dispatch  = wf_a0_ms           (DBOS queue wait)
        setup     = wf_setup_ms        (manifest/DSN resolve)
        ingest    = wf_ingest_ms       (run_ingest = source.run + facts
                                        INSERT + publisher cycle inside)
        close     = wf_close_ms        (DBOS post-step cleanup)

    `cycle_ms` from the gen table is NESTED inside the ingest span (the
    publisher cycle runs inline in run_ingest, not as a separate
    workflow), so we overlay it as a thin line on top of the stack
    rather than stacking it additively. Older runs without the setup
    probe collapse `wf_setup_ms` to the legacy A1 envelope and skip
    the ingest band.
    """
    x = _i_array(rows)
    if len(rows) > 100:
        idx = np.linspace(0, len(rows) - 1, 100, dtype=int)
        rows_s = [rows[i] for i in idx]
        x_s = np.array([r["i"] for r in rows_s], dtype=float)
    else:
        rows_s = rows
        x_s = x
    dispatch = np.nan_to_num(_arr(rows_s, "wf_a0_ms"),    nan=0.0)
    setup    = np.nan_to_num(_arr(rows_s, "wf_setup_ms"), nan=0.0)
    ingest   = np.nan_to_num(_arr(rows_s, "wf_ingest_ms"),nan=0.0)
    close    = np.nan_to_num(_arr(rows_s, "wf_close_ms"), nan=0.0)
    cyc      = np.nan_to_num(_arr(rows_s, "cycle_ms"),    nan=0.0)
    # Legacy fallback: if every row has ingest=0 and wf_setup_ms came
    # from the old "first-step minus started" definition, the bench
    # ran before the @DBOS.step probe landed. Relabel the band as
    # "body (legacy)" so the reader knows it's the old combined number.
    has_ingest = bool(np.any(ingest > 0))
    if has_ingest:
        bands = [dispatch, setup, ingest, close]
        labels = [
            "dispatch wait (A0)",
            "setup (manifest+DSN+path)",
            "ingest (run_ingest)",
            "close (post-step cleanup)",
        ]
        colors = ["#E69F00", "#56B4E9", "#009E73", "#999999"]
    else:
        bands = [dispatch, setup, close]
        labels = [
            "dispatch wait (A0)",
            "body up to setEvent  (legacy A1 — no setup probe)",
            "close (post-step cleanup)",
        ]
        colors = ["#E69F00", "#56B4E9", "#999999"]
    ax.stackplot(x_s, *bands, labels=labels, colors=colors, alpha=0.85)
    # Overlay publisher cycle as a thin reference line — it's NESTED
    # inside the ingest band, not an additional contribution.
    ax.plot(
        x_s, cyc, color="#222222", linewidth=0.9, linestyle=":",
        label="publisher cycle (nested in ingest)",
    )
    ax.set_xlabel("commit index (chronological)")
    ax.set_ylabel("ms")
    ax.set_title("phase breakdown (non-overlapping)")
    ax.legend(loc="upper right", frameon=True, fontsize=6)


def _draw_summary_text(ax, exp_id, rows):
    """Text-panel: experiment metadata + bench summary stats. This is
    the 6th slot in the 2x3 grid; gives the reader a one-glance
    summary alongside the plots."""
    ax.axis("off")
    n = len(rows)
    e2e = [r["e2e_ms"] for r in rows if not math.isnan(r["e2e_ms"])]
    cyc = [r["cycle_ms"] for r in rows if not math.isnan(r["cycle_ms"])]
    last = rows[-1] if rows else {}
    n_live = last.get("nodes_live", float("nan"))
    e_live = last.get("edges_live", float("nan"))
    def _fmt(v):
        return "—" if (v is None or (isinstance(v, float) and math.isnan(v))) \
            else f"{v:.0f}"
    lines = [
        f"experiment    : {exp_id}",
        f"commits       : {n}",
        "",
        f"e2e p50 / p95 : "
        f"{_fmt(np.percentile(e2e, 50) if e2e else float('nan'))} / "
        f"{_fmt(np.percentile(e2e, 95) if e2e else float('nan'))} ms",
        f"e2e p99 / max : "
        f"{_fmt(np.percentile(e2e, 99) if e2e else float('nan'))} / "
        f"{_fmt(max(e2e) if e2e else float('nan'))} ms",
        f"cycle p50/p95 : "
        f"{_fmt(np.percentile(cyc, 50) if cyc else float('nan'))} / "
        f"{_fmt(np.percentile(cyc, 95) if cyc else float('nan'))} ms",
        "",
        f"final graph   : "
        f"nodes_live={_fmt(n_live)}, edges_live={_fmt(e_live)}",
    ]
    ax.text(
        0.02, 0.98, "\n".join(lines),
        family="monospace", fontsize=8, va="top", ha="left",
        transform=ax.transAxes,
    )
    ax.set_title("summary", loc="left")


def plot_config_grid(exp_id: str, rows: list[dict]) -> Path:
    """One PDF per experiment, 2x3 grid of useful panels:

         row 0 :  latency_timeline   latency_CDF       latency_histogram
         row 1 :  phase_breakdown    graph_growth      cycle_vs_graphsize
         row 2 :  summary text       (spare)           (spare)

    Replaces the prior 5-individual-PDFs-per-experiment layout —
    one figure per config is the unit you actually want to look at.
    """
    repo = exp_id.split("-", 1)[0]
    color = color_for_repo(repo)
    fig, axes = plt.subplots(3, 3, figsize=(15.0, 11.0))
    # Row 0 — latency
    _draw_latency_timeline(axes[0, 0], exp_id, rows, color)
    _draw_latency_cdf(axes[0, 1], rows, color)
    _draw_latency_hist(axes[0, 2], rows, color)
    # Row 1 — operations
    _draw_phase_breakdown(axes[1, 0], rows)
    _draw_graph_growth(axes[1, 1], rows, color)
    _draw_cycle_vs_graphsize(axes[1, 2], rows, color)
    # Row 2 — summary (left), hide the rest
    _draw_summary_text(axes[2, 0], exp_id, rows)
    axes[2, 1].axis("off")
    axes[2, 2].axis("off")
    fig.suptitle(f"{exp_id}", fontsize=12, y=0.995)
    PLOTS_PER.mkdir(parents=True, exist_ok=True)
    path = PLOTS_PER / f"{exp_id}__grid.pdf"
    fig.tight_layout(rect=[0, 0, 1, 0.985])
    fig.savefig(path, format="pdf")
    plt.close(fig)
    return path


def per_experiment(exp_id: str) -> list[Path]:
    rows = load_csv(exp_id)
    if not rows:
        print(f"[{exp_id}] no CSV; skip")
        return []
    out = [plot_config_grid(exp_id, rows)]
    print(f"[{exp_id}] wrote {len(out)} PDF")
    for p in out:
        print(f"  {p.relative_to(ROOT)}")
    return out


# ---------------------------------------------------------------------------
# Cross-cutting plots

def cross_repo_latency_cdf(exps, target_set: str = "files+history") -> Optional[Path]:
    """CDFs of one source set across all repos."""
    pairs: list[tuple[str, str, np.ndarray]] = []
    for e in exps:
        if e.source_set != target_set:
            continue
        rows = load_csv(e.id)
        if not rows:
            continue
        y = _arr(rows, "e2e_ms")
        y = y[~np.isnan(y)]
        if len(y) == 0:
            continue
        pairs.append((e.repo.name, e.id, np.sort(y)))
    if not pairs:
        return None
    fig, ax = plt.subplots()
    for repo, exp_id, ys in pairs:
        cdf = np.arange(1, len(ys) + 1) / len(ys)
        ax.plot(ys, cdf, color=color_for_repo(repo), linewidth=1.5,
                label=repo)
    ax.set_xlabel("e2e latency (ms)")
    ax.set_ylabel("empirical CDF")
    ax.set_title(
        f"cross-repo latency CDF (source set: {target_set})"
    )
    ax.set_ylim(0, 1.02)
    ax.legend(loc="lower right", frameon=True)
    slug = target_set.replace("+", "_").replace("-", "_")
    return _save_cross(f"cross_repo_latency_cdf_{slug}", fig)


def cross_repo_throughput(exps) -> Optional[Path]:
    target_set = "files+history"
    data: list[tuple[str, float]] = []
    for e in exps:
        if e.source_set != target_set:
            continue
        rows = load_csv(e.id)
        if not rows:
            continue
        total_ms = sum(
            r["e2e_ms"] for r in rows if not math.isnan(r["e2e_ms"])
        )
        if total_ms == 0:
            continue
        commits_per_min = len(rows) * 60_000 / total_ms
        data.append((e.repo.name, commits_per_min))
    if not data:
        return None
    fig, ax = plt.subplots()
    repos = [d[0] for d in data]
    vals = [d[1] for d in data]
    colors = [color_for_repo(r) for r in repos]
    bars = ax.bar(repos, vals, color=colors, edgecolor="black", linewidth=0.4)
    for bar, v in zip(bars, vals, strict=True):
        ax.text(bar.get_x() + bar.get_width() / 2, v * 1.02,
                f"{v:.0f}", ha="center", va="bottom", fontsize=9)
    ax.set_ylabel("commits / minute")
    ax.set_title(f"throughput by repo ({target_set})")
    return _save_cross("cross_repo_throughput", fig)


def cross_config_latency_for_repo(repo_name: str, exps) -> Optional[Path]:
    """CDFs of e2e for one repo, across the three source sets."""
    pairs: list[tuple[str, np.ndarray]] = []
    for e in exps:
        if e.repo.name != repo_name:
            continue
        rows = load_csv(e.id)
        if not rows:
            continue
        y = _arr(rows, "e2e_ms")
        y = y[~np.isnan(y)]
        if len(y) == 0:
            continue
        pairs.append((e.source_set, np.sort(y)))
    if len(pairs) < 2:
        return None
    fig, ax = plt.subplots()
    color = color_for_repo(repo_name)
    for src_set, ys in pairs:
        cdf = np.arange(1, len(ys) + 1) / len(ys)
        ax.plot(
            ys, cdf,
            color=color,
            linestyle=linestyle_for_source_set(src_set),
            linewidth=1.6,
            label=label_for_source_set(src_set),
        )
    ax.set_xlabel("e2e latency (ms)")
    ax.set_ylabel("empirical CDF")
    ax.set_title(f"latency by source set — {repo_name}")
    ax.set_ylim(0, 1.02)
    ax.legend(loc="lower right", frameon=True)
    return _save_cross(f"cross_config_latency_{repo_name}", fig)


def cross_repo_graph_growth(exps) -> Optional[Path]:
    """Log-log scatter of final graph size vs commits replayed,
    one point per (repo, source_set) experiment that ran.
    """
    points = []
    for e in exps:
        rows = load_csv(e.id)
        if not rows:
            continue
        last = rows[-1]
        n_live = last["nodes_live"]
        e_live = last["edges_live"]
        if math.isnan(n_live) or math.isnan(e_live):
            continue
        total = n_live + e_live
        if total <= 0:
            continue
        points.append((e.repo.name, e.source_set, len(rows), total))
    if not points:
        return None
    fig, ax = plt.subplots()
    seen_legend: set[str] = set()
    for repo, src_set, n_commits, total in points:
        marker = {"files-only": "o", "history-only": "s",
                  "files+history": "^"}.get(src_set, "x")
        label = f"{repo} ({src_set})"
        ax.scatter(
            n_commits, total,
            color=color_for_repo(repo),
            marker=marker, s=60,
            edgecolors="black", linewidths=0.5,
            label=label if label not in seen_legend else None,
        )
        seen_legend.add(label)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("commits replayed")
    ax.set_ylabel("final nodes_live + edges_live")
    ax.set_title("graph size vs commits replayed (all experiments)")
    ax.legend(loc="lower right", frameon=True, fontsize=7)
    return _save_cross("cross_repo_graph_growth", fig)


def _save_cross(name: str, fig) -> Path:
    PLOTS_CROSS.mkdir(parents=True, exist_ok=True)
    path = PLOTS_CROSS / f"{name}.pdf"
    fig.tight_layout()
    fig.savefig(path, format="pdf")
    plt.close(fig)
    return path


def cross_all(exps) -> list[Path]:
    out: list[Path] = []
    # Cross-repo CDF, one per source set (was only files+history before;
    # uninformative when comparing just one repo).
    for src_set in ("files-only", "history-only", "files+history"):
        p = cross_repo_latency_cdf(exps, target_set=src_set)
        if p:
            out.append(p)
    for fn in (cross_repo_throughput, cross_repo_graph_growth):
        p = fn(exps)
        if p:
            out.append(p)
    for repo in {e.repo.name for e in exps}:
        p = cross_config_latency_for_repo(repo, exps)
        if p:
            out.append(p)
    return out


# ---------------------------------------------------------------------------
# CLI

def main() -> int:
    apply_paper_style()
    exps = _load_manifest()
    args = sys.argv[1:]
    if not args:
        sys.exit("usage: plot.py <experiment_id> | --all | --cross")
    if "--cross" in args:
        out = cross_all(exps)
        print(f"cross: wrote {len(out)} PDFs")
        for p in out:
            print(f"  {p.relative_to(ROOT)}")
        return 0
    if "--all" in args:
        produced = 0
        for e in exps:
            produced += len(per_experiment(e.id))
        out = cross_all(exps)
        print(f"all: wrote {produced} per-experiment + {len(out)} cross PDFs")
        return 0
    # Single experiment id
    per_experiment(args[0])
    return 0


if __name__ == "__main__":
    sys.exit(main())
