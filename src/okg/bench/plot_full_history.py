#!/usr/bin/env python3
"""Full-history per-commit plots.

Takes the bench results CSVs for the three tinydb full-history configs
and produces one grid PDF with:

  1. Sync time per commit (e2e_ms vs commit_index), with rolling p50/p95
  2. Graph growth (nodes_live + edges_live vs commit_index, twin axis)
  3. DB size (pg_database_size in MB vs commit_index)
  4. Delta size vs sync time (scatter: lines_changed → e2e_ms)

Plus useful extras:

  5. Phase breakdown (stacked area: dispatch/setup/ingest/close per commit)
  6. Publisher cycle vs e2e ratio (how much of wall is publisher work)
  7. Rolling throughput (commits/min over a 30-commit window)

All overlaid by source-set (files-only / history-only / files+history)
where applicable.

Run AFTER the three full-history experiments complete.

Usage:
    python bench/plot_full_history.py
"""
from __future__ import annotations

import csv
import contextlib
import math
import os
import sys
from pathlib import Path

try:
    import matplotlib.pyplot as plt
    import numpy as np
except ImportError as _plot_exc:  # pragma: no cover - wheel test
    raise ImportError(
        "okg.bench.plot_full_history needs matplotlib — install the bench extra: "
        "pip install 'okg[bench]'"
    ) from _plot_exc

_BENCH = Path(os.environ.get("OKG_BENCH_ROOT", Path.cwd() / "bench")).resolve()
ROOT = _BENCH.parent
RESULTS = _BENCH / "results"

# CLI:
#   plot_full_history.py [suffix] [repo]
#   examples:
#     plot_full_history.py 100             → tinydb-*-100 (default)
#     plot_full_history.py 100 tinydb      → same
#     plot_full_history.py 100 palimpzest  → palimpzest-tier1-100 only
SUFFIX = sys.argv[1] if len(sys.argv) > 1 else "100"
REPO   = sys.argv[2] if len(sys.argv) > 2 else "tinydb"
SUFFIX_TAG = f"-{SUFFIX}" if SUFFIX else ""
OUT_PDF = ROOT / "bench" / "plots" / f"{REPO}_full_history{SUFFIX_TAG}.pdf"

if REPO == "tinydb":
    CONFIGS = [
        (f"tinydb-files-only{SUFFIX_TAG}",    "files-only",     "#0072B2", "-"),
        (f"tinydb-history-only{SUFFIX_TAG}",  "history-only",   "#D55E00", "--"),
        (f"tinydb-files-history{SUFFIX_TAG}", "files+history",  "#009E73", "-."),
        (f"tinydb-files-code{SUFFIX_TAG}",    "files+code",     "#CC79A7", ":"),
        (f"tinydb-all{SUFFIX_TAG}",           "all (f+h+code)", "#000000", "-"),
    ]
elif REPO == "palimpzest":
    CONFIGS = [
        (f"palimpzest-tier1{SUFFIX_TAG}",
         "tier1 (6 src)", "#0072B2", "-"),
    ]
else:
    sys.exit(f"unknown repo {REPO!r}; pick tinydb or palimpzest")


def _coerce(r: dict) -> dict:
    out = dict(r)
    for k in (
        "i", "files_changed", "lines_added", "lines_deleted",
        "e2e_ms", "cycle_ms", "nodes_added", "edges_added",
        "nodes_live", "edges_live", "gen_id", "db_size_bytes",
        "wf_a0_ms", "wf_setup_ms", "wf_ingest_ms", "wf_close_ms",
    ):
        v = out.get(k, "")
        if v in ("", "NaN", None):
            out[k] = float("nan")
        else:
            try:
                out[k] = int(v)
            except (ValueError, TypeError):
                with contextlib.suppress(ValueError, TypeError):
                    out[k] = float(v)
    return out


def load(exp_id: str) -> list[dict]:
    p = RESULTS / f"{exp_id}.csv"
    if not p.exists():
        print(f"  warn: {p} missing — skipping {exp_id}", file=sys.stderr)
        return []
    with p.open() as fh:
        return [_coerce(r) for r in csv.DictReader(fh)]


def arr(rows, key):
    return np.array([r.get(key, float("nan")) for r in rows], dtype=float)


def rolling(x: np.ndarray, w: int) -> np.ndarray:
    """Centered rolling mean. Returns NaN for the partial-window
    edges so the plot doesn't suggest data we don't have."""
    out = np.full_like(x, np.nan, dtype=float)
    if len(x) < w:
        return out
    cs = np.cumsum(np.insert(x, 0, 0.0))
    for i in range(w - 1, len(x)):
        out[i] = (cs[i + 1] - cs[i + 1 - w]) / w
    return out


def panel_sync_time(ax):
    """Plot 1: per-commit sync time (e2e_ms) over commit index."""
    for cfg_id, label, color, ls in CONFIGS:
        rows = load(cfg_id)
        if not rows:
            continue
        x = arr(rows, "i")
        y = arr(rows, "e2e_ms")
        # Drop the queue-race rows (e2e < 50 ms is the bench
        # measurement artifact, not a real per-commit timing).
        mask = y >= 50
        ax.plot(x[mask], y[mask], ".", color=color, markersize=1.6,
                alpha=0.45, label=f"{label} (raw)")
        # Rolling p50 of e2e_ms over a 25-commit window
        if mask.sum() > 25:
            roll = rolling(y[mask], 25)
            ax.plot(x[mask], roll, "-", color=color, linewidth=1.4,
                    label=f"{label} (rolling p50, w=25)")
    ax.set_xlabel("commit index (chronological)")
    ax.set_ylabel("end-to-end latency (ms)")
    ax.set_title("Plot 1 — sync time per commit")
    ax.legend(loc="upper right", frameon=True, fontsize=7, ncol=3)


def panel_graph_growth(ax_n):
    """Plot 2: nodes_live + edges_live over commit index, twin axis.

    Three configs overlaid:
      - files-only: nodes_live grows, edges_live ≡ 0
      - history-only: edges_live grows fast, nodes_live grows slowly
      - files+history: both grow

    Color = source-set; linestyle = nodes (solid) vs edges (dotted).
    """
    ax_e = ax_n.twinx()
    for cfg_id, label, color, ls in CONFIGS:
        rows = load(cfg_id)
        if not rows:
            continue
        x = arr(rows, "i")
        n = arr(rows, "nodes_live")
        e = arr(rows, "edges_live")
        ax_n.plot(x, n, "-", color=color, linewidth=1.4,
                  label=f"{label} nodes")
        ax_e.plot(x, e, ":", color=color, linewidth=1.2,
                  alpha=0.85, label=f"{label} edges")
    ax_n.set_xlabel("commit index (chronological)")
    ax_n.set_ylabel("nodes_live")
    ax_e.set_ylabel("edges_live  (dotted)")
    ax_n.set_title("Plot 2 — graph growth")
    h1, l1 = ax_n.get_legend_handles_labels()
    h2, l2 = ax_e.get_legend_handles_labels()
    ax_n.legend(h1 + h2, l1 + l2, loc="upper left",
                frameon=True, fontsize=6, ncol=2)


def panel_db_size(ax):
    """Plot 3: pg_database_size over commit index."""
    for cfg_id, label, color, ls in CONFIGS:
        rows = load(cfg_id)
        if not rows:
            continue
        x = arr(rows, "i")
        size = arr(rows, "db_size_bytes")
        # Convert to MB for readability
        mb = size / (1024 * 1024)
        ax.plot(x, mb, "-", color=color, linewidth=1.4, label=label)
    ax.set_xlabel("commit index (chronological)")
    ax.set_ylabel("Postgres DB size (MB)")
    ax.set_title("Plot 3 — DB size growth")
    ax.legend(loc="upper left", frameon=True, fontsize=7)


def panel_delta_vs_sync(ax):
    """Plot 4: scatter of delta size (lines_changed) vs sync time."""
    for cfg_id, label, color, ls in CONFIGS:
        rows = load(cfg_id)
        if not rows:
            continue
        delta = arr(rows, "lines_added") + arr(rows, "lines_deleted")
        e2e = arr(rows, "e2e_ms")
        # Drop queue-race rows
        mask = (e2e >= 50) & (delta > 0) & np.isfinite(delta) & np.isfinite(e2e)
        ax.scatter(delta[mask], e2e[mask], color=color, s=10,
                   alpha=0.4, edgecolors="none", label=label)
    ax.set_xlabel("delta size (lines_added + lines_deleted)")
    ax.set_ylabel("end-to-end latency (ms)")
    ax.set_xscale("symlog")
    ax.set_yscale("log")
    ax.set_title("Plot 4 — delta size vs sync time")
    ax.legend(loc="upper left", frameon=True, fontsize=7)


def panel_phase_breakdown(ax, cfg_id: str | None = None):
    """Plot 5 (extra): stacked-area phase breakdown for one config.

    Defaults to the 3rd entry in CONFIGS (files+history, the richest
    case) at the active SUFFIX, so the panel always reads the same
    data set the other panels are showing.
    """
    if cfg_id is None:
        # Default to the heaviest config in the current set; falls
        # back to the only config if there's just one.
        cfg_id = CONFIGS[min(2, len(CONFIGS) - 1)][0]
    rows = load(cfg_id)
    if not rows:
        ax.text(0.5, 0.5, "(no data)", ha="center", va="center",
                transform=ax.transAxes)
        return
    x = arr(rows, "i")
    if len(rows) > 200:
        idx = np.linspace(0, len(rows) - 1, 200, dtype=int)
        rs = [rows[i] for i in idx]
        xs = arr(rs, "i")
    else:
        rs = rows
        xs = x
    dispatch = np.nan_to_num(arr(rs, "wf_a0_ms"),    nan=0.0)
    setup    = np.nan_to_num(arr(rs, "wf_setup_ms"), nan=0.0)
    ingest   = np.nan_to_num(arr(rs, "wf_ingest_ms"), nan=0.0)
    close    = np.nan_to_num(arr(rs, "wf_close_ms"), nan=0.0)
    has_ingest = bool(np.any(ingest > 0))
    if has_ingest:
        ax.stackplot(
            xs, dispatch, setup, ingest, close,
            labels=["dispatch (A0)", "setup", "ingest (run_ingest)", "close"],
            colors=["#E69F00", "#56B4E9", "#009E73", "#999999"],
            alpha=0.85,
        )
    else:
        ax.stackplot(
            xs, dispatch, setup, close,
            labels=["dispatch (A0)", "body→setEvent (legacy A1)", "close"],
            colors=["#E69F00", "#56B4E9", "#999999"],
            alpha=0.85,
        )
    ax.set_xlabel("commit index (chronological)")
    ax.set_ylabel("ms")
    ax.set_title(f"Plot 5 — phase breakdown ({cfg_id})")
    ax.legend(loc="upper right", frameon=True, fontsize=6)


def panel_cycle_fraction(ax):
    """Plot 6 (extra): publisher cycle_ms / e2e_ms ratio over commits.

    Reveals how much of wall is publisher work vs workflow envelope
    and source emission. Steady ratio → flat overhead. Rising ratio
    on big-delta commits → publisher is the bottleneck.
    """
    for cfg_id, label, color, ls in CONFIGS:
        rows = load(cfg_id)
        if not rows:
            continue
        x = arr(rows, "i")
        cyc = arr(rows, "cycle_ms")
        e2e = arr(rows, "e2e_ms")
        mask = (e2e >= 50) & (cyc > 0) & np.isfinite(cyc / e2e)
        ratio = cyc[mask] / e2e[mask]
        ax.plot(x[mask], ratio, "-", color=color, linewidth=1.0,
                alpha=0.7, label=label)
    ax.set_xlabel("commit index (chronological)")
    ax.set_ylabel("publisher_cycle / e2e")
    ax.set_ylim(0, 1.0)
    ax.set_title("Plot 6 — publisher cycle as fraction of wall")
    ax.legend(loc="upper right", frameon=True, fontsize=7)


def panel_throughput(ax):
    """Plot 7 (extra): rolling throughput (commits per minute).

    Uses a 30-commit centered window. Captures both steady-state
    throughput and degradation if the publisher cycle slows down
    as the graph grows.
    """
    for cfg_id, label, color, ls in CONFIGS:
        rows = load(cfg_id)
        if not rows:
            continue
        x = arr(rows, "i")
        e2e = arr(rows, "e2e_ms")
        mask = e2e >= 50
        if mask.sum() < 30:
            continue
        # Rolling-window throughput: 30 commits / sum(e2e in those 30)
        e2e_clean = e2e[mask]
        x_clean = x[mask]
        w = 30
        tput = np.full_like(e2e_clean, np.nan, dtype=float)
        for i in range(w - 1, len(e2e_clean)):
            window_ms = e2e_clean[i + 1 - w:i + 1].sum()
            if window_ms > 0:
                tput[i] = (w / window_ms) * 60_000  # commits/min
        ax.plot(x_clean, tput, "-", color=color, linewidth=1.2,
                label=label)
    ax.set_xlabel("commit index (chronological)")
    ax.set_ylabel("commits / minute  (30-window rolling)")
    ax.set_title("Plot 7 — rolling throughput")
    ax.legend(loc="upper right", frameon=True, fontsize=7)


def panel_phases_vs_commit(ax):
    """Plot 8: each workflow phase as a separate rolling line vs
    commit index, so we can see which phase is scaling with graph
    size.  Uses the heaviest config in CONFIGS (last one)."""
    cfg_id, label, color, _ls = CONFIGS[-1]
    rows = load(cfg_id)
    if not rows:
        ax.text(0.5, 0.5, "(no data)", ha="center", va="center",
                transform=ax.transAxes)
        return
    x = arr(rows, "i")
    series = [
        ("wf_a0_ms",      "DBOS dispatch (A0)", "#E69F00"),
        ("wf_setup_ms",   "setup",              "#56B4E9"),
        ("wf_ingest_ms",  "ingest (run_ingest)", "#009E73"),
        ("wf_close_ms",   "close",              "#999999"),
        ("cycle_ms",      "publisher cycle (nested in ingest)", "#000000"),
    ]
    for key, lbl, color in series:
        y = arr(rows, key)
        # Rolling p50 over a 15-commit centered window to denoise.
        w = 15
        if len(y) >= w:
            roll = rolling(y, w)
            ax.plot(x, roll, "-", color=color, linewidth=1.4,
                    label=lbl, alpha=0.85)
        else:
            ax.plot(x, y, "-", color=color, linewidth=1.4, label=lbl)
    ax.set_xlabel("commit index (chronological)")
    ax.set_ylabel("rolling p50 (ms, w=15)")
    ax.set_title("Plot 8 — per-phase scaling with commit index")
    ax.legend(loc="upper left", frameon=True, fontsize=7)


def panel_summary_text(ax):
    """Plot 8: per-config summary text (counts + percentiles)."""
    ax.axis("off")
    lines = ["per-config summary (post-pygit2)", ""]
    lines.append(f"{'config':22s}  {'commits':>7}  {'p50_ms':>7}  "
                 f"{'p95_ms':>7}  {'nodes':>6}  {'edges':>6}  {'DB MB':>7}")
    for cfg_id, label, color, ls in CONFIGS:
        rows = load(cfg_id)
        if not rows:
            continue
        e2e = [r["e2e_ms"] for r in rows
               if not math.isnan(r["e2e_ms"]) and r["e2e_ms"] >= 50]
        last = rows[-1]
        size_mb = (last.get("db_size_bytes", 0) or 0) / (1024 * 1024)
        if not isinstance(size_mb, float):
            size_mb = 0
        p50 = int(np.percentile(e2e, 50)) if e2e else 0
        p95 = int(np.percentile(e2e, 95)) if e2e else 0
        lines.append(
            f"{label:22s}  {len(rows):>7}  {p50:>7}  {p95:>7}  "
            f"{int(last.get('nodes_live') or 0):>6}  "
            f"{int(last.get('edges_live') or 0):>6}  "
            f"{size_mb:>7.1f}"
        )
    ax.text(0.02, 0.98, "\n".join(lines), family="monospace",
            fontsize=8, va="top", ha="left", transform=ax.transAxes)


def main() -> int:
    OUT_PDF.parent.mkdir(parents=True, exist_ok=True)
    # 5-row × 2-col grid: 4 requested + 4 extras + 1 summary panel
    fig, axes = plt.subplots(5, 2, figsize=(15.0, 20.0))
    panel_sync_time(axes[0, 0])
    panel_graph_growth(axes[0, 1])
    panel_db_size(axes[1, 0])
    panel_delta_vs_sync(axes[1, 1])
    panel_phase_breakdown(axes[2, 0])
    panel_cycle_fraction(axes[2, 1])
    panel_throughput(axes[3, 0])
    panel_phases_vs_commit(axes[3, 1])
    panel_summary_text(axes[4, 0])
    axes[4, 1].axis("off")
    title = (
        f"{REPO}-{SUFFIX} replay (post-pygit2 in substrate + bench, "
        f"pool, dirty-scoped alias seed, 10 ms watcher debounce, "
        f"reconcile pass at startup)"
    )
    fig.suptitle(title, fontsize=12, y=0.995)
    fig.tight_layout(rect=[0, 0, 1, 0.985])
    fig.savefig(OUT_PDF, format="pdf")
    plt.close(fig)
    print(f"Wrote {OUT_PDF}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
