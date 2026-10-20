#!/usr/bin/env python3
"""Render the single-commit per-source breakdown into one PDF.

Two panels in a single PDF:

  TOP — horizontal stacked bar, one bar per source, showing the
        wall_ms split into git-subprocess time vs the rest
        (Python + facts INSERT + projection + publisher gates).
        For non-git sources, "git-subprocess time" is 0 and the
        whole bar is "rest".

  BOTTOM — for each source that issued >0 git calls, a horizontal
        bar showing per-`run_git`-call durations, labeled with the
        argv head. Reveals which git invocations dominate the
        ~95-120 ms subprocess floor.

Run after `single_commit_breakdown.py`.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

try:
    import matplotlib.pyplot as plt
    import numpy as np
except ImportError as _plot_exc:  # pragma: no cover - wheel test
    raise ImportError(
        "okg.bench.per_source.plot_breakdown needs matplotlib — install the bench extra: "
        "pip install 'okg[bench]'"
    ) from _plot_exc

_BENCH = Path(os.environ.get("OKG_BENCH_ROOT", Path.cwd() / "bench")).resolve()
RESULTS = _BENCH / "per_source" / "results" / "single_commit_breakdown.json"
OUT_PDF = _BENCH / "per_source" / "plots" / "single_commit_breakdown.pdf"

# Compact colors that match bench/style.py's palette.
COLORS = {
    "git":      "#E69F00",
    "rest":     "#56B4E9",
    "ls-files": "#D55E00",
    "ls-tree":  "#CC79A7",
    "log":      "#0072B2",
    "diff":     "#009E73",
    "status":   "#F0E442",
    "rev-parse": "#999999",
    "default":  "#777777",
}


def color_for_argv(argv):
    if not argv:
        return COLORS["default"]
    head = argv[0]
    return COLORS.get(head, COLORS["default"])


def short_label(argv):
    return " ".join(str(a) for a in argv[:3])


def main() -> int:
    if not RESULTS.exists():
        sys.exit(f"missing {RESULTS}; run single_commit_breakdown.py first")
    rows = json.loads(RESULTS.read_text())
    rows = [r for r in rows if "error" not in r]
    if not rows:
        sys.exit("no successful rows in results JSON")

    OUT_PDF.parent.mkdir(parents=True, exist_ok=True)

    fig, (ax_top, ax_bot) = plt.subplots(
        2, 1, figsize=(11.0, 9.0),
        gridspec_kw={"height_ratios": [1, 2]},
    )

    # --- Top: stacked bar of wall_ms = git_subprocess + rest ---
    names = [r["source"] for r in rows]
    wall = np.array([r["wall_ms_total"] for r in rows], dtype=float)
    git_ms = np.array([r.get("git_calls_total_ms") or 0.0 for r in rows])
    rest = wall - git_ms
    y = np.arange(len(rows))

    ax_top.barh(y, git_ms, color=COLORS["git"], label="git subprocess")
    ax_top.barh(y, rest, left=git_ms, color=COLORS["rest"],
                label="rest (Python + INSERT + projection + gates)")
    for i, r in enumerate(rows):
        ax_top.text(wall[i] + max(wall) * 0.01, i,
                    f"{wall[i]:.0f} ms  "
                    f"({r['git_calls_count']} git calls, "
                    f"{r.get('facts_emitted_in_report', 0)} facts, "
                    f"nodes_Δ={r['nodes_delta']:+d}, "
                    f"edges_Δ={r['edges_delta']:+d})",
                    va="center", fontsize=8)
    ax_top.set_yticks(y)
    ax_top.set_yticklabels(names)
    ax_top.set_xlabel("wall-clock ms for run_ingest(scope_incomplete, commit-4→commit-5)")
    ax_top.set_title("Single-commit per-source breakdown — tinydb commit 4 → 5",
                     fontsize=11)
    ax_top.legend(loc="lower right", frameon=True, fontsize=8)
    ax_top.invert_yaxis()
    ax_top.set_xlim(0, max(wall) * 1.45)

    # --- Bottom: per-git-call stacked bar per source ---
    git_rows = [r for r in rows if r.get("git_calls")]
    if git_rows:
        y2 = np.arange(len(git_rows))
        for i, r in enumerate(git_rows):
            x = 0.0
            for call in r["git_calls"]:
                ms = call["elapsed_ms"]
                argv = call["argv"]
                c = color_for_argv(argv)
                ax_bot.barh(i, ms, left=x, color=c, edgecolor="white",
                            linewidth=0.5)
                ax_bot.text(x + ms / 2, i, short_label(argv),
                            ha="center", va="center", fontsize=6,
                            color="black", rotation=0,
                            clip_on=True)
                x += ms
            ax_bot.text(x + 2, i,
                        f"  total {r['git_calls_total_ms']:.0f} ms  "
                        f"({r['git_calls_count']} calls)",
                        va="center", fontsize=8)
        ax_bot.set_yticks(y2)
        ax_bot.set_yticklabels([r["source"] for r in git_rows])
        ax_bot.set_xlabel("git-subprocess time per call (ms)")
        ax_bot.set_title("Per-`run_git` call breakdown (git-sourced ones)",
                         fontsize=11)
        ax_bot.invert_yaxis()
    else:
        ax_bot.text(0.5, 0.5, "(no git-sourced rows in results)",
                    ha="center", va="center", transform=ax_bot.transAxes)
        ax_bot.axis("off")

    fig.tight_layout()
    fig.savefig(OUT_PDF, format="pdf")
    plt.close(fig)
    print(f"Wrote {OUT_PDF}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
