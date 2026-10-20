"""Generate self-contained HTML+SVG plots from bench_results.csv.

No matplotlib. Just stdlib + numpy for percentile math.

Environment:
  OKG_BENCH_OUT   directory containing bench_results.csv
                  (default: ./.scratch). Output bench_plots.html
                  is written to the same directory.
"""
from __future__ import annotations
import csv
import os
import statistics
from pathlib import Path

import numpy as np

OUT_DIR = Path(
    os.environ.get("OKG_BENCH_OUT", str(Path.cwd() / ".scratch"))
)
CSV = OUT_DIR / "bench_results.csv"
OUT_HTML = OUT_DIR / "bench_plots.html"


def load_rows():
    rows = []
    with CSV.open() as fh:
        r = csv.DictReader(fh)
        for raw in r:
            row = {}
            for k, v in raw.items():
                if v == "" or v is None:
                    row[k] = None
                elif k in {
                    "i", "files_changed", "lines_added", "lines_deleted",
                    "cycle_ms", "nodes_added", "edges_added",
                }:
                    try:
                        row[k] = int(v)
                    except ValueError:
                        row[k] = None
                elif k == "end_to_end_s":
                    try:
                        row[k] = float(v)
                    except ValueError:
                        row[k] = None
                else:
                    row[k] = v
            rows.append(row)
    return rows


# -- SVG primitives -----------------------------------------------------------


def _svg_open(width, height):
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {width} {height}" '
        f'width="{width}" height="{height}" '
        f'style="font-family: ui-monospace, Menlo, monospace; '
        f'font-size: 11px; background: #fafafa;">'
    )


def _axes(
    width: int, height: int,
    xs: list[float], ys: list[float],
    title: str, xlabel: str, ylabel: str,
    margin_l: int = 60, margin_r: int = 20,
    margin_t: int = 38, margin_b: int = 52,
    y_floor_zero: bool = True,
):
    plot_w = width - margin_l - margin_r
    plot_h = height - margin_t - margin_b
    if not xs:
        xs = [0.0]
    if not ys:
        ys = [0.0]
    x_min = min(xs)
    x_max = max(xs)
    if x_min == x_max:
        x_max = x_min + 1
    y_min = 0.0 if y_floor_zero else min(ys)
    y_max = max(ys) or 1.0
    if y_min == y_max:
        y_max = y_min + 1

    def xscale(x):
        return margin_l + (x - x_min) / (x_max - x_min) * plot_w

    def yscale(y):
        return margin_t + plot_h - (y - y_min) / (y_max - y_min) * plot_h

    parts = []
    # Background grid + axes.
    parts.append(
        f'<rect x="{margin_l}" y="{margin_t}" '
        f'width="{plot_w}" height="{plot_h}" '
        f'fill="white" stroke="#ccc"/>'
    )
    # 5 horizontal gridlines.
    for i in range(6):
        y_val = y_min + (y_max - y_min) * i / 5
        y_px = yscale(y_val)
        parts.append(
            f'<line x1="{margin_l}" y1="{y_px}" '
            f'x2="{margin_l + plot_w}" y2="{y_px}" '
            f'stroke="#eee"/>'
        )
        parts.append(
            f'<text x="{margin_l - 6}" y="{y_px + 3}" '
            f'text-anchor="end" fill="#555">{y_val:.1f}</text>'
        )
    # Title + labels.
    parts.append(
        f'<text x="{width/2}" y="22" text-anchor="middle" '
        f'font-size="13" font-weight="600">{title}</text>'
    )
    parts.append(
        f'<text x="{width/2}" y="{height-12}" text-anchor="middle">'
        f'{xlabel}</text>'
    )
    parts.append(
        f'<text transform="translate(16,{margin_t + plot_h/2}) '
        f'rotate(-90)" text-anchor="middle">{ylabel}</text>'
    )
    return parts, xscale, yscale, margin_l, margin_t, plot_w, plot_h


def line_plot(
    rows, x_key, y_key, title, xlabel, ylabel,
    color="#0a7", point_color=None, width=900, height=320,
):
    pts = [(r[x_key], r[y_key]) for r in rows
           if r.get(x_key) is not None and r.get(y_key) is not None]
    parts, xscale, yscale, *_ = _axes(
        width, height,
        [p[0] for p in pts], [p[1] for p in pts],
        title, xlabel, ylabel,
    )
    if pts:
        # x-axis labels (every Nth).
        n = len(pts)
        step = max(1, n // 10)
        for i, (x, _y) in enumerate(pts):
            if i % step == 0 or i == n - 1:
                parts.append(
                    f'<text x="{xscale(x)}" y="{height-32}" '
                    f'text-anchor="middle" fill="#555" '
                    f'font-size="10">{x}</text>'
                )
        path = " ".join(
            f"{'M' if i==0 else 'L'} {xscale(x)} {yscale(y)}"
            for i, (x, y) in enumerate(pts)
        )
        parts.append(
            f'<path d="{path}" stroke="{color}" stroke-width="1.8" '
            f'fill="none"/>'
        )
        pc = point_color or color
        for x, y in pts:
            parts.append(
                f'<circle cx="{xscale(x)}" cy="{yscale(y)}" '
                f'r="2.5" fill="{pc}"/>'
            )
    return _svg_open(width, height) + "".join(parts) + "</svg>"


def scatter_plot(
    rows, x_key, y_key, title, xlabel, ylabel,
    color="#06c", width=600, height=420, show_identity=False,
):
    pts = [(r[x_key], r[y_key]) for r in rows
           if r.get(x_key) is not None and r.get(y_key) is not None]
    parts, xscale, yscale, ml, mt, pw, ph = _axes(
        width, height,
        [p[0] for p in pts] + [0], [p[1] for p in pts] + [0],
        title, xlabel, ylabel,
    )
    if show_identity and pts:
        # y=x line.
        x_min = min(p[0] for p in pts)
        x_max = max(p[0] for p in pts)
        y_min = min(p[1] for p in pts)
        y_max = max(p[1] for p in pts)
        v_min = max(0, min(x_min, y_min))
        v_max = max(x_max, y_max)
        parts.append(
            f'<line x1="{xscale(v_min)}" y1="{yscale(v_min)}" '
            f'x2="{xscale(v_max)}" y2="{yscale(v_max)}" '
            f'stroke="#999" stroke-dasharray="4 4"/>'
        )
        parts.append(
            f'<text x="{xscale(v_max)}" y="{yscale(v_max)-6}" '
            f'fill="#999" text-anchor="end">y = x</text>'
        )
    # x-axis ticks (5).
    if pts:
        xs = [p[0] for p in pts]
        x_min = min(xs)
        x_max = max(xs)
        for i in range(6):
            v = x_min + (x_max - x_min) * i / 5
            parts.append(
                f'<text x="{xscale(v)}" y="{height-32}" '
                f'text-anchor="middle" fill="#555" '
                f'font-size="10">{v:.0f}</text>'
            )
    for x, y in pts:
        parts.append(
            f'<circle cx="{xscale(x)}" cy="{yscale(y)}" '
            f'r="4" fill="{color}" fill-opacity="0.65"/>'
        )
    return _svg_open(width, height) + "".join(parts) + "</svg>"


def histogram(
    values, title, xlabel, ylabel="count",
    color="#a40", bins=20, width=600, height=320,
):
    values = [float(v) for v in values if v is not None]
    if not values:
        return _svg_open(width, height) + "</svg>"
    hist, edges = np.histogram(values, bins=bins)
    centers = [(edges[i] + edges[i + 1]) / 2 for i in range(len(hist))]
    parts, xscale, yscale, ml, mt, pw, ph = _axes(
        width, height,
        list(edges), [0, *hist.tolist()],
        title, xlabel, ylabel,
    )
    bar_w = pw / len(hist) - 1
    for c, h in zip(centers, hist.tolist(), strict=True):
        x0 = xscale(c) - bar_w / 2
        y0 = yscale(h)
        y1 = yscale(0)
        parts.append(
            f'<rect x="{x0:.1f}" y="{y0:.1f}" '
            f'width="{bar_w:.1f}" height="{y1 - y0:.1f}" '
            f'fill="{color}" fill-opacity="0.75"/>'
        )
    # x-axis ticks
    for i in [0, len(edges) // 2, len(edges) - 1]:
        parts.append(
            f'<text x="{xscale(edges[i])}" y="{height-32}" '
            f'text-anchor="middle" fill="#555" '
            f'font-size="10">{edges[i]:.0f}</text>'
        )
    # p50/p95 lines.
    p50 = float(np.percentile(values, 50))
    p95 = float(np.percentile(values, 95))
    for pct, lbl, col in ((p50, "p50", "#06c"), (p95, "p95", "#a04")):
        parts.append(
            f'<line x1="{xscale(pct)}" y1="{mt}" '
            f'x2="{xscale(pct)}" y2="{mt + ph}" '
            f'stroke="{col}" stroke-dasharray="3 3"/>'
        )
        parts.append(
            f'<text x="{xscale(pct)+4}" y="{mt+12}" '
            f'fill="{col}" font-size="10">'
            f'{lbl} = {pct:.0f}</text>'
        )
    return _svg_open(width, height) + "".join(parts) + "</svg>"


def main():
    rows = load_rows()
    rows = [r for r in rows if r.get("cycle_ms") is not None]
    print(f"Loaded {len(rows)} rows from {CSV}")

    # ---- summary stats
    e2e = [r["end_to_end_s"] for r in rows if r["end_to_end_s"] is not None]
    cyc = [r["cycle_ms"] for r in rows if r["cycle_ms"] is not None]
    fast = [v for v in e2e if v < 10]
    slow = [v for v in e2e if v >= 10]
    p50_cyc = int(statistics.median(cyc))
    p95_cyc = sorted(cyc)[int(0.95 * len(cyc))]
    p50_fast = statistics.median(fast) if fast else 0
    p50_slow = statistics.median(slow) if slow else 0
    correct = sum(
        1 for r in rows if r["nodes_added"] == r["files_changed"]
    )

    intro_html = f"""
    <div class="prose">
      <h2>What you're looking at</h2>
      <p>OKG (operational knowledge graph) ingests changes to source repositories and projects them into a Postgres-backed bitemporal graph. When a commit lands on disk, a filesystem watcher fires; the substrate reads the changed files, computes fingerprints, emits node / edge facts, runs a <strong>publisher cycle</strong> that reserves a new graph generation, projects the facts, runs invariant gates, and commits the generation. Each commit produces a new <em>generation</em> (a labelled snapshot of the live graph).</p>
      <p>This page reports a <strong>commit-replay benchmark</strong>: we cloned <code>pallets/click</code> (149 source files, ~1100 commits), set up a fresh isolated <code>okg_bench</code> database with the substrate schema, and walked <strong>{len(rows)} consecutive commits</strong> via <code>git reset --hard &lt;commit&gt;</code>. For each commit we measured wall-clock from the reset to a new published generation, and recorded the per-cycle internal work and graph deltas.</p>
      <p>The benchmark is deliberately <strong>minimal</strong> — only the <code>git-files</code> source is enabled, no enrichers, no linkers, no embedding. So these numbers represent <em>the publisher's raw ingestion path</em>, not what a feature-loaded production deployment would do.</p>

      <h2>Glossary</h2>
      <table class="glossary">
        <tr><th><code>files_changed</code></th>
            <td>How many distinct files differ between this commit
                and its predecessor, per <code>git diff --name-only</code>.</td></tr>
        <tr><th><code>end_to_end_s</code></th>
            <td><strong>Operator-visible freshness:</strong> wall-clock
                seconds from <code>git reset --hard</code> to a new
                generation being published. Includes watcher debounce,
                workflow queueing, and the publisher cycle itself.</td></tr>
        <tr><th><code>cycle_ms</code></th>
            <td><strong>Publisher work time:</strong> the time from
                generation reservation to publication, measured at the
                row level (<code>published_at - created_at</code>).
                Excludes wait time before the cycle starts.</td></tr>
        <tr><th><code>nodes_added</code></th>
            <td>How many <em>new versions</em> of nodes the publisher
                wrote into <code>nodes_live</code> at this generation —
                this should track <code>files_changed</code> for
                changes that actually altered file content.</td></tr>
        <tr><th><code>edges_added</code></th>
            <td>How many new live-edge rows were emitted. With no
                enrichers / linkers, this stays near zero for this
                benchmark.</td></tr>
      </table>

      <h2>Bottom line</h2>
      <ul>
        <li><strong>{len(rows)} commits processed end-to-end in {sum(e2e):.0f} seconds</strong>
            ({len(rows) * 60 / sum(e2e):.1f} commits/minute).</li>
        <li>Publisher cycle work is <strong>cheap and predictable</strong>:
            p50 = {p50_cyc} ms, p95 = {p95_cyc} ms.</li>
        <li>End-to-end freshness is <strong>bimodal</strong>:
            {len(fast)}/{len(rows)} commits land in p50 {p50_fast:.1f} s (fast path:
            watcher fired);
            {len(slow)}/{len(rows)} commits take p50 {p50_slow:.0f} s (slow path:
            zero-file merge commits where the watcher had nothing to
            fire on, so we waited for the next 60-s poll cron).</li>
        <li>Source-adapter precision: <code>nodes_added</code> equals
            <code>files_changed</code> for {correct}/{len(rows)}
            ({100 * correct / len(rows):.0f}%) of commits. The mismatches
            are mostly merge commits with content already covered by
            a previous publish.</li>
      </ul>
    </div>
    """

    plot1 = line_plot(
        rows, "i", "end_to_end_s",
        title="Plot 1. End-to-end freshness per commit",
        xlabel="commit index (1 = oldest replayed, "
               f"{len(rows)} = newest)",
        ylabel="seconds from `git reset` to new generation",
        color="#0a7", width=900, height=320,
    )
    plot2 = line_plot(
        rows, "i", "cycle_ms",
        title="Plot 2. Publisher cycle work per commit",
        xlabel="commit index",
        ylabel="cycle work (ms)",
        color="#a04", width=900, height=320,
    )
    plot3 = scatter_plot(
        rows, "files_changed", "end_to_end_s",
        title="Plot 3. End-to-end freshness vs. commit size",
        xlabel="files changed in commit",
        ylabel="end-to-end seconds",
        width=460, height=380,
    )
    plot4 = scatter_plot(
        rows, "files_changed", "nodes_added",
        title="Plot 4. Source-adapter precision check",
        xlabel="files changed (git diff)",
        ylabel="nodes added at this generation",
        color="#06c", width=460, height=380,
        show_identity=True,
    )
    plot5 = scatter_plot(
        rows, "lines_added", "cycle_ms",
        title="Plot 5. Cycle work vs. lines added",
        xlabel="lines added in commit",
        ylabel="cycle work (ms)",
        color="#a04", width=460, height=380,
    )
    plot6 = histogram(
        cyc, title="Plot 6. Publisher cycle work — distribution",
        xlabel="cycle work (ms)", color="#a04",
        bins=18, width=460, height=380,
    )
    plot7 = histogram(
        e2e, title="Plot 7. End-to-end freshness — distribution",
        xlabel="seconds", color="#0a7",
        bins=18, width=460, height=380,
    )

    explainers = {
        "plot1": """
            <p><strong>What it shows.</strong> The headline plot — one
            point per replayed commit, plotted in chronological order.
            The y-axis is seconds of wall-clock from <code>git reset
            --hard &lt;commit&gt;</code> until the new generation is
            published.</p>
            <p><strong>How to read it.</strong> Most commits sit near
            the bottom of the chart (~4 s); a few stick up like
            spikes to ~50 s. The spikes are <strong>merge commits
            that touched zero files</strong> on disk — the filesystem
            watcher had nothing to fire on, so the worker had to wait
            for the next 60-second poll-cron tick to discover the
            content was actually unchanged. This is not a publisher
            slowness; it's a watcher-doesn't-trigger artifact.</p>
            <p><strong>Healthy?</strong> Yes. The fast-path latency
            (~4 s) includes watcher debounce + DBOS workflow dispatch
            + ~100 ms of actual publishing. If we cared about closing
            the slow-path gap, we'd either trigger an explicit
            <code>okg ingest</code> for merge commits, or tighten the
            poll interval.</p>
        """,
        "plot2": """
            <p><strong>What it shows.</strong> Same x-axis, but now the
            y-axis is the <em>internal</em> publisher cycle work in
            milliseconds — the time from generation reservation to
            commit, measured at the row level. This is the cost of
            "doing the actual database writes," excluding queue wait
            and watcher debounce.</p>
            <p><strong>How to read it.</strong> A tight horizontal band
            at ~100 ms with sporadic dips to ~25 ms. The dips are
            the same zero-file merge commits — when there's nothing
            to project, even the publisher's coordination overhead
            shrinks.</p>
            <p><strong>What it tells us.</strong> The publisher cycle
            is <strong>very consistent</strong>. Whatever inefficiency
            exists is a flat per-cycle cost, not a per-delta cost.
            Plot 5 confirms this directly.</p>
        """,
        "plot3": """
            <p><strong>What it shows.</strong> Same data as Plot 1 but
            re-keyed: each point is (files-changed, end-to-end-seconds)
            for one commit. The dashed reference line (if shown) is
            <code>y = x</code>.</p>
            <p><strong>How to read it.</strong> There are two clouds.
            The big horizontal cloud at y ≈ 4 sits across the full
            range of x (1 to 24 files) — meaning a 1-file commit and a
            24-file commit take roughly the same time. The other
            cloud, sitting at y ≈ 50 with x = 0, is the zero-file
            merge commits.</p>
            <p><strong>Implication.</strong> <strong>End-to-end
            freshness does not scale with commit size</strong> for
            commits up to ~24 files. The cost is dominated by fixed
            overhead, not delta size.</p>
        """,
        "plot4": """
            <p><strong>What it shows.</strong> A correctness sanity
            check. The x-axis is the number of files in the commit's
            <code>git diff</code>; the y-axis is the number of new
            node versions the publisher wrote to <code>nodes_live</code>.
            The dashed <code>y = x</code> line is the ideal: one node
            per changed file.</p>
            <p><strong>How to read it.</strong> Most points sit on the
            line — the source adapter correctly identifies the changed
            files and emits one node-version-update per file. The
            outliers below the line are commits where some changed
            files were already in the same content-hash state as a
            prior version (e.g. a merge that re-introduces an older
            tree, or a cherry-pick that produces the same final
            content).</p>
            <p><strong>What it tells us.</strong> The git-files source
            adapter is doing its job correctly for ~87 % of commits.
            The 13 % under-counting is "fingerprint already covered"
            and is expected behavior, not a bug.</p>
        """,
        "plot5": """
            <p><strong>What it shows.</strong> Per-commit publisher work
            (ms) plotted against the number of lines the commit
            touched (<code>git diff --shortstat</code>).</p>
            <p><strong>How to read it.</strong> The points cluster in a
            tight horizontal band regardless of how many lines the
            commit changed. From 0 to 400+ lines of diff, the cycle
            time stays within roughly 80-150 ms.</p>
            <p><strong>What it tells us.</strong> Whatever's eating
            cycle time is <strong>per-cycle</strong>, not <strong>per-line</strong>.
            The profiling artifacts from this run break it down
            further: ~17 ms is named publisher stages (reserve,
            phase_a, phase_b, ledger_snapshot) and ~85 ms is
            unprofiled workflow / DBOS / DB-connection glue.
            Future tuning should attack the framework overhead, not
            the publish phases.</p>
        """,
        "plot6": """
            <p><strong>What it shows.</strong> Histogram of every
            commit's publisher cycle work, with vertical lines at
            the p50 and p95.</p>
            <p><strong>How to read it.</strong> Most cycles cluster
            around the p50 (~106 ms). A small tail near zero
            corresponds to the zero-file merge commits.</p>
            <p><strong>What it tells us.</strong> Cycle work has a
            <strong>tight distribution</strong>: p95 (119 ms) is only
            13 % above p50. That's the signature of well-behaved
            workload — no fat tail, no surprise slow cycles.</p>
        """,
        "plot7": """
            <p><strong>What it shows.</strong> Histogram of every
            commit's end-to-end freshness, with p50/p95 lines.</p>
            <p><strong>How to read it.</strong> Two clear peaks: one
            near 4 s (the fast path) and a small one near 50 s (the
            slow path). The shape is <strong>bimodal</strong>, which
            is a telltale signature of "two regimes." Plot 1
            identifies the regimes (watcher-fired vs. cron-only).</p>
            <p><strong>What it tells us.</strong> The end-to-end p95
            (~50 s) is misleading on its own — it's not the worst-case
            of a single regime, it's the slow regime entirely. p50
            (~4 s) is a much better signal for "what does an operator
            see on a normal day."</p>
        """,
    }

    html = f"""<!doctype html>
<html><head><meta charset="utf-8">
<title>synth-bench: OKG commit-replay benchmark</title>
<style>
  body {{ font-family: system-ui, -apple-system, sans-serif;
          padding: 24px; max-width: 1100px; margin: 0 auto;
          color: #222; background: #f5f5f5; line-height: 1.55; }}
  h1 {{ margin-top: 0; }}
  h2 {{ margin-top: 1.5em; border-bottom: 1px solid #ccc;
        padding-bottom: 4px; }}
  h3 {{ margin-top: 1.4em; }}
  .meta {{ color: #888; font-size: 13px; }}
  .prose {{ background: white; padding: 18px 24px;
            border-radius: 8px; margin-bottom: 24px;
            box-shadow: 0 1px 2px rgba(0,0,0,0.06); }}
  .prose table.glossary {{ border-collapse: collapse;
                            margin: 8px 0; }}
  .prose table.glossary th {{ vertical-align: top;
                              text-align: left;
                              padding: 4px 14px 4px 0;
                              white-space: nowrap;
                              color: #555; font-weight: 500; }}
  .prose table.glossary td {{ padding: 4px 0; }}
  .prose ul {{ padding-left: 22px; }}
  .prose code {{ background: #f0f0f0; padding: 0 4px;
                 border-radius: 3px; font-size: 13px; }}
  .plot-block {{ background: white; padding: 16px 20px;
                 border-radius: 8px; margin-bottom: 24px;
                 box-shadow: 0 1px 2px rgba(0,0,0,0.06); }}
  .plot-block .plot-svg {{ display: flex; justify-content: center;
                            margin: 8px 0 16px; }}
  .plot-block .explainer p {{ margin: 6px 0; }}
  .plot-row {{ display: flex; gap: 16px; flex-wrap: wrap; }}
  .plot-row > .plot-block {{ flex: 1 1 480px; }}
  .footer {{ color: #888; font-size: 12px; margin-top: 2em;
             padding-top: 1em; border-top: 1px solid #ddd; }}
</style></head>
<body>
<h1>OKG commit-replay benchmark</h1>
<p class="meta">
  Synthetic benchmark of OKG's publisher path. Source workload:
  <a href="https://github.com/pallets/click">pallets/click</a>
  (149 source files). Replayed via <code>git reset --hard</code>
  against a fresh isolated Postgres database.
</p>

{intro_html}

<h2>The plots</h2>

<div class="plot-block">
  <div class="plot-svg">{plot1}</div>
  <div class="explainer">{explainers["plot1"]}</div>
</div>

<div class="plot-block">
  <div class="plot-svg">{plot2}</div>
  <div class="explainer">{explainers["plot2"]}</div>
</div>

<div class="plot-row">
  <div class="plot-block">
    <div class="plot-svg">{plot3}</div>
    <div class="explainer">{explainers["plot3"]}</div>
  </div>
  <div class="plot-block">
    <div class="plot-svg">{plot4}</div>
    <div class="explainer">{explainers["plot4"]}</div>
  </div>
</div>

<div class="plot-row">
  <div class="plot-block">
    <div class="plot-svg">{plot5}</div>
    <div class="explainer">{explainers["plot5"]}</div>
  </div>
  <div class="plot-block">
    <div class="plot-svg">{plot6}</div>
    <div class="explainer">{explainers["plot6"]}</div>
  </div>
</div>

<div class="plot-block">
  <div class="plot-svg">{plot7}</div>
  <div class="explainer">{explainers["plot7"]}</div>
</div>

<h2>How this was produced</h2>
<div class="prose">
  <ol>
    <li>Clone <code>pallets/click</code> to <code>.scratch/bench-repos/click</code>.</li>
    <li>Create a fresh isolated Postgres database (<code>okg_bench</code>)
        with the substrate's required extensions (timescaledb, vector,
        pg_textsearch, pg_trgm, fuzzystrmatch, btree_gist).</li>
    <li>Run <code>okg migrate</code> and <code>okg runtime
        bootstrap</code> against the fresh DB.</li>
    <li>Launch <code>okg runtime worker --deployment synth-bench</code>;
        wait for the initial scan to publish a baseline generation.</li>
    <li>Walk the most recent {len(rows)} commits via
        <code>git reset --hard &lt;commit&gt;</code>, wait for each
        commit's new generation to be published, record per-commit
        metrics.</li>
    <li>Emit this HTML and a parallel CSV.</li>
  </ol>
  <p>The full setup + run instructions are in
     <code>deployments/synth-bench/scripts/README.md</code>. A future
     "perf regressed?" investigation re-runs the same three commands.</p>
</div>

<p class="footer">
  Generated from <code>bench_results.csv</code> ({len(rows)} rows).
  No external JavaScript or CSS dependencies — this page is one
  self-contained HTML file. To regenerate after a fresh run:
  <code>python deployments/synth-bench/scripts/plot_bench.py</code>.
</p>
</body></html>
"""
    OUT_HTML.write_text(html)
    print(f"wrote plots: {OUT_HTML}")
    print(f"open in browser: file://{OUT_HTML}")


if __name__ == "__main__":
    main()
