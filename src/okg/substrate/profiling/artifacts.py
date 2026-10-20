from __future__ import annotations

import json
import logging
import os
import re
import shutil
from html import escape
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .model import ArtifactRef, ProfileRun

logger = logging.getLogger(__name__)


def profile_root() -> Path:
    """Where profiles go: OKG_PROFILE_DIR, else okg's user data folder.

    The default used to be `./profiles`, so every `okg install`, ingest and
    publish left `profiles/<deployment>/_live/*.json` in whatever folder the
    operator ran it from (their home folder, in a newcomer's case). It is now
    `profiles/` beside `deployments/` under
    `${OKG_USER_HOME:-${XDG_DATA_HOME:-~/.local/share}/okg}`.
    """
    override = os.environ.get("OKG_PROFILE_DIR")
    if override:
        return Path(override).expanduser().resolve()
    from okg.substrate.deployments import _user_data_dir

    return (_user_data_dir() / "profiles").resolve()


def deployment_root(deployment: str) -> Path:
    return profile_root() / deployment


def live_dir(deployment: str) -> Path:
    return deployment_root(deployment) / "_live"


def ensure_profile_root() -> Path:
    """Create the profile root.

    The default root lives in okg's user data folder, outside any working
    directory or repository, so it needs no `.gitignore`. A root the
    operator chose with OKG_PROFILE_DIR is only created as well: it may be a
    directory, even a repository root, whose contents they track.
    """
    root = profile_root()
    root.mkdir(parents=True, exist_ok=True)
    return root


def write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True))
    tmp.replace(path)


def load_profile(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def list_profiles(deployment: str | None = None) -> list[dict[str, Any]]:
    roots = [deployment_root(deployment)] if deployment else [
        p for p in profile_root().iterdir() if p.is_dir()
    ] if profile_root().exists() else []
    out: list[dict[str, Any]] = []
    for root in roots:
        for f in root.glob("*/profile.json"):
            try:
                data = load_profile(f)
            except Exception:  # noqa: BLE001 — listing tolerates one corrupt profile.json; debug-log + skip.
                logger.debug("skipping unreadable profile %s", f, exc_info=True)
                continue
            data["_path"] = str(f)
            out.append(data)
    out.sort(key=lambda r: r.get("started_at") or "", reverse=True)
    return out


def safe_profile_id(value: str) -> str:
    if not re.match(r"^[A-Za-z0-9_.:-]+$", value):
        raise ValueError("invalid profile id")
    return value


def find_profile(profile_id: str, deployment: str | None = None) -> Path | None:
    profile_id = safe_profile_id(profile_id)
    roots = [deployment_root(deployment)] if deployment else [
        p for p in profile_root().iterdir() if p.is_dir()
    ] if profile_root().exists() else []
    for root in roots:
        candidate = root / profile_id / "profile.json"
        if candidate.is_file():
            return candidate
    return None


def profile_dir(run: ProfileRun) -> Path:
    return deployment_root(run.deployment) / run.profile_run_id


def add_artifact(run: ProfileRun, path: Path, *, kind: str, media_type: str) -> None:
    try:
        rel = str(path.relative_to(profile_dir(run)))
    except ValueError:
        rel = path.name
    run.artifacts.append(ArtifactRef(path=rel, kind=kind, media_type=media_type))


def write_profile_bundle(run: ProfileRun) -> Path:
    ensure_profile_root()
    root = profile_dir(run)
    root.mkdir(parents=True, exist_ok=True)
    _enforce_retention(run.deployment, run.policy)
    generated_paths = {
        "profile.json",
        "timeline.json",
        "queries.json",
        "trace_events.json",
        "summary.md",
        "bottleneck_report.md",
        "report.html",
    }
    existing_artifacts = [
        artifact for artifact in run.artifacts
        if artifact.path not in generated_paths
        and not artifact.path.startswith("plots/")
    ]
    base_artifacts = [
        ArtifactRef("profile.json", "application/json", "profile"),
        ArtifactRef("timeline.json", "application/json", "timeline"),
        ArtifactRef("queries.json", "application/json", "queries"),
        ArtifactRef("trace_events.json", "application/json", "trace-events"),
        ArtifactRef("summary.md", "text/markdown", "summary"),
        ArtifactRef("bottleneck_report.md", "text/markdown", "bottleneck-report"),
        ArtifactRef("report.html", "text/html", "bottleneck-report-html"),
    ]
    seen = {artifact.path for artifact in base_artifacts}
    run.artifacts = base_artifacts
    for artifact in existing_artifacts:
        if artifact.path not in seen:
            run.artifacts.append(artifact)
            seen.add(artifact.path)
    _write_plots(run, root)
    _dedupe_artifacts(run)
    write_json_atomic(root / "profile.json", run.to_dict())
    write_json_atomic(root / "timeline.json", {
        "stages": [s.__dict__ | {"duration_ms": s.duration_ms} for s in run.stages],
    })
    write_json_atomic(root / "queries.json", {
        "queries": [q.__dict__ | {"duration_ms": q.duration_ms} for q in run.queries],
    })
    write_json_atomic(root / "trace_events.json", _trace_events(run))
    summary = _summary_markdown(run)
    (root / "summary.md").write_text(summary)
    (root / "bottleneck_report.md").write_text(_bottleneck_markdown(run))
    (root / "report.html").write_text(_bottleneck_html(run))
    _enforce_retention(run.deployment, run.policy)
    return root


def _dedupe_artifacts(run: ProfileRun) -> None:
    seen: set[str] = set()
    deduped: list[ArtifactRef] = []
    for artifact in run.artifacts:
        if artifact.path in seen:
            continue
        seen.add(artifact.path)
        deduped.append(artifact)
    run.artifacts = deduped


def _write_plots(run: ProfileRun, root: Path) -> None:
    try:
        from PIL import Image, ImageDraw, ImageFont
    except Exception:  # noqa: BLE001 — pillow is an optional dep; record limitation and skip plot generation.
        run.limitations.append("plot artifacts unavailable: pillow not installed")
        return

    analysis = _analyze_profile(run)
    plot_dir = root / "plots"
    plot_dir.mkdir(parents=True, exist_ok=True)

    def font(size: int, *, bold: bool = False) -> Any:
        for path in (
            "/System/Library/Fonts/Supplemental/Arial Bold.ttf" if bold else
            "/System/Library/Fonts/Supplemental/Arial.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        ):
            try:
                return ImageFont.truetype(path, size=size)
            except Exception:  # noqa: BLE001 — font file missing on this OS; try the next candidate.
                logger.debug("font unavailable: %s", path, exc_info=True)
                continue
        return ImageFont.load_default()

    title_font = font(22, bold=True)
    subtitle_font = font(14)
    label_font = font(14)
    small_font = font(12)
    mono_font = font(13)

    def new_canvas(
        title: str,
        subtitle: str | None = None,
        *,
        width: int = 1600,
        height: int = 980,
    ) -> tuple[Any, Any]:
        image = Image.new("RGB", (width, height), "white")
        draw = ImageDraw.Draw(image)
        draw.text((40, 30), title, fill="#111827", font=title_font)
        if subtitle:
            draw.text((40, 62), subtitle, fill="#6b7280", font=subtitle_font)
        return image, draw

    def save(image: Any, name: str, kind: str) -> None:
        path = plot_dir / name
        image.save(path)
        add_artifact(run, path, kind=kind, media_type="image/png")

    def empty(draw: Any, message: str) -> None:
        draw.text((40, 110), message, fill="#6b7280", font=label_font)

    def clip(value: str, max_chars: int) -> str:
        return value if len(value) <= max_chars else f"{value[:max_chars - 3]}..."

    def ms(value: float) -> str:
        if value >= 1000:
            return f"{value / 1000:.2f}s"
        return f"{value:.1f}ms"

    def draw_axis(
        draw: Any,
        *,
        left: int,
        top: int,
        width: int,
        max_value: float,
        label: str = "seconds",
    ) -> None:
        draw.line((left, top, left + width, top), fill="#d1d5db", width=1)
        ticks = 5
        for i in range(ticks + 1):
            x = left + int(width * i / ticks)
            value = max_value * i / ticks
            draw.line((x, top - 4, x, top + 4), fill="#d1d5db", width=1)
            draw.text((x - 18, top + 10), f"{value / 1000:.1f}", fill="#6b7280", font=small_font)
        draw.text((left + width + 12, top + 10), label, fill="#6b7280", font=small_font)

    def wrap_text(value: str, width: int) -> list[str]:
        words = value.split()
        lines: list[str] = []
        current: list[str] = []
        for word in words:
            candidate = " ".join((*current, word))
            if len(candidate) > width and current:
                lines.append(" ".join(current))
                current = [word]
            else:
                current.append(word)
        if current:
            lines.append(" ".join(current))
        return lines

    def draw_card(
        draw: Any,
        box: tuple[int, int, int, int],
        label: str,
        value: str,
        note: str,
    ) -> None:
        x0, y0, x1, y1 = box
        draw.rectangle(box, fill="#f9fafb", outline="#e5e7eb", width=1)
        draw.text((x0 + 18, y0 + 14), label, fill="#6b7280", font=small_font)
        draw.text((x0 + 18, y0 + 42), value, fill="#111827", font=font(26, bold=True))
        draw.text((x0 + 18, y0 + 82), note, fill="#374151", font=small_font)

    def query_class(query: dict[str, Any]) -> str:
        if query["calls"] >= 100:
            return "batch writes"
        if query["avg_ms"] >= 100:
            return "plan/index"
        return "minor"

    def write_dashboard() -> None:
        image, draw = new_canvas(
            "Bottleneck Dashboard",
            "Decision view: what is slow, why it is slow, and which fix class applies.",
            width=1800,
            height=1250,
        )
        summary = run.summary
        duration = run.duration_ms or 0.0
        db_ms = summary.db_round_trip_ms if summary else 0.0
        db_pct = (db_ms / duration * 100.0) if duration else 0.0
        top_stage = summary.top_stage if summary else "unknown"
        high_call = [q for q in analysis["many_small_queries"] if q["calls"] >= 100]
        high_call_ms = sum(q["total_ms"] for q in high_call)
        high_call_count = sum(q["calls"] for q in high_call)
        gate_stage = next(
            (r for r in analysis["top_stage_rows"] if r["stage"] == "publisher.gates"),
            None,
        )
        phase_stage = next(
            (r for r in analysis["top_stage_rows"] if r["stage"] == "publisher.phase_a"),
            None,
        )
        draw_card(
            draw,
            (40, 110, 430, 230),
            "total publish",
            ms(duration),
            f"profile {run.profile_run_id}",
        )
        draw_card(
            draw,
            (460, 110, 850, 230),
            "postgres round trip",
            f"{ms(db_ms)} ({db_pct:.1f}%)",
            "optimize SQL before Python",
        )
        draw_card(
            draw,
            (880, 110, 1270, 230),
            "top stage",
            str(top_stage),
            f"{ms(summary.top_stage_ms) if summary else 'n/a'} wall time",
        )
        draw_card(
            draw,
            (1300, 110, 1690, 230),
            "batching candidate",
            f"{high_call_count:,} calls",
            f"{ms(high_call_ms)} across high-call SQL",
        )

        draw.text((40, 285), "Critical Path", fill="#111827", font=font(20, bold=True))
        path_rows = [
            ("reserve", _stage_wall(analysis, "publisher.reserve"), "#6b7280"),
            ("phase A / enrichers", phase_stage["wall_ms"] if phase_stage else 0.0, "#2563eb"),
            ("gates", gate_stage["wall_ms"] if gate_stage else 0.0, "#dc2626"),
            ("ledger", _stage_wall(analysis, "publisher.ledger_snapshot"), "#6b7280"),
        ]
        total_path = sum(value for _, value, _ in path_rows) or 1.0
        x = 40
        y = 330
        total_w = 1160
        for label, value, color in path_rows:
            w = max(2, int(total_w * value / total_path))
            draw.rectangle((x, y, x + w, y + 38), fill=color)
            if value >= 100:
                draw.text((x + 8, y + 50), label, fill="#111827", font=small_font)
                draw.text((x + 8, y + 70), ms(value), fill="#6b7280", font=small_font)
            x += w + 3
        draw.text(
            (1240, 326),
            "This run is effectively two large DB-bound blocks: Phase A/enrichers and gates.",
            fill="#374151",
            font=label_font,
        )
        draw.text(
            (1240, 356),
            "The bars are sequential top-level publisher time, not inclusive nested totals.",
            fill="#6b7280",
            font=small_font,
        )
        draw.text(
            (1240, 382),
            "Reserve and ledger are both under 10ms and are intentionally unlabeled in the bar.",
            fill="#6b7280",
            font=small_font,
        )

        draw.text((40, 470), "Fix Class 1: Single Slow Gate SQL", fill="#111827", font=font(20, bold=True))
        draw.text((40, 500), "These need EXPLAIN plans, indexes, or query rewrites.", fill="#6b7280", font=small_font)
        gate_queries = [
            q for q in analysis["top_queries"]
            if q["stage"] == "publisher.gates" and q["calls"] <= 3
        ][:4]
        max_gate = max((q["total_ms"] for q in gate_queries), default=1.0)
        for idx, q in enumerate(gate_queries):
            yy = 540 + idx * 118
            draw.text((60, yy), q["fingerprint"], fill="#111827", font=mono_font)
            draw.text((250, yy), f"{ms(q['total_ms'])} / {q['calls']} call", fill="#111827", font=small_font)
            bar_w = int(450 * q["total_ms"] / max_gate)
            draw.rectangle((430, yy, 430 + bar_w, yy + 20), fill="#dc2626")
            for line_idx, line in enumerate(wrap_text(_clip_sql(q["sql"], 210), 95)[:2]):
                draw.text((60, yy + 32 + line_idx * 18), line, fill="#374151", font=small_font)

        draw.text((930, 470), "Fix Class 2: Many Small Writes", fill="#111827", font=font(20, bold=True))
        draw.text((930, 500), "These need set-based writes or batching.", fill="#6b7280", font=small_font)
        batch_queries = high_call[:4]
        max_batch = max((q["total_ms"] for q in batch_queries), default=1.0)
        for idx, q in enumerate(batch_queries):
            yy = 540 + idx * 118
            draw.text((950, yy), q["fingerprint"], fill="#111827", font=mono_font)
            draw.text(
                (1140, yy),
                f"{ms(q['total_ms'])} / {q['calls']:,} calls / avg {ms(q['avg_ms'])}",
                fill="#111827",
                font=small_font,
            )
            bar_w = int(450 * q["total_ms"] / max_batch)
            draw.rectangle((950, yy + 28, 950 + bar_w, yy + 48), fill="#2563eb")
            for line_idx, line in enumerate(wrap_text(_clip_sql(q["sql"], 200), 82)[:2]):
                draw.text((950, yy + 60 + line_idx * 18), line, fill="#374151", font=small_font)

        draw.text((40, 1060), "Priority", fill="#111827", font=font(20, bold=True))
        priority = [
            "1. EXPLAIN the two slow publisher.gates queries.",
            "2. Batch structural observation emitter writes; three fingerprints account for ~4.9s.",
            "3. Ignore Python tuning until DB round-trip stops dominating the profile.",
        ]
        for idx, line in enumerate(priority):
            draw.text((60, 1100 + idx * 30), line, fill="#111827", font=label_font)
        save(image, "bottleneck_dashboard.png", "bottleneck-dashboard")

    def write_query_pareto() -> None:
        image, draw = new_canvas(
            "Query Pareto",
            "Top SQL shapes with fix class and SQL snippet. This is the plot to use for next-action triage.",
            width=1900,
            height=1400,
        )
        rows = analysis["top_queries"][:12]
        max_value = max((q["total_ms"] for q in rows), default=1.0)
        left = 520
        top = 138
        width = 580
        row_h = 96
        draw_axis(draw, left=left, top=112, width=width, max_value=max_value)
        headers = [("class", 1120), ("total", 1280), ("calls", 1380), ("avg", 1500)]
        for text, x in headers:
            draw.text((x, 108), text, fill="#6b7280", font=small_font)
        colors = {"plan/index": "#dc2626", "batch writes": "#2563eb", "minor": "#6b7280"}
        for idx, q in enumerate(rows):
            y = top + idx * row_h
            cls = query_class(q)
            draw.text((44, y), q["fingerprint"], fill="#111827", font=mono_font)
            draw.text((44, y + 22), clip(q["stage"], 54), fill="#6b7280", font=small_font)
            for line_idx, line in enumerate(wrap_text(_clip_sql(q["sql"], 170), 72)[:2]):
                draw.text((44, y + 46 + line_idx * 17), line, fill="#374151", font=small_font)
            bar_w = int(width * q["total_ms"] / max_value)
            draw.rectangle((left, y, left + bar_w, y + 22), fill=colors[cls])
            draw.text((1120, y), cls, fill=colors[cls], font=small_font)
            draw.text((1280, y), ms(q["total_ms"]), fill="#111827", font=small_font)
            draw.text((1380, y), f"{q['calls']:,}", fill="#111827", font=small_font)
            draw.text((1500, y), ms(q["avg_ms"]), fill="#111827", font=small_font)
        save(image, "query_pareto.png", "query-pareto")

    write_dashboard()
    write_query_pareto()

    def metric_bar_chart(
        title: str,
        rows: list[dict[str, Any]],
        name: str,
        kind: str,
        subtitle: str | None = None,
        label_key: str = "label",
        value_key: str = "value",
        extra_columns: tuple[str, ...] = (),
    ) -> None:
        image, draw = new_canvas(title, subtitle)
        rows = [row for row in rows if row.get(value_key, 0) > 0][:10]
        if not rows:
            empty(draw, "No measurements recorded.")
            save(image, name, kind)
            return
        max_value = max(float(row[value_key]) for row in rows) or 1.0
        label_x = 44
        bar_x = 420
        top = 132
        axis_y = 108
        bar_w_max = 700
        row_h = 64
        col_x = [1160, 1260, 1360, 1460]
        draw_axis(draw, left=bar_x, top=axis_y, width=bar_w_max, max_value=max_value)
        if extra_columns:
            headers = {"total": "total", "calls": "calls", "avg": "avg"}
            for col_idx, col in enumerate(extra_columns):
                draw.text(
                    (col_x[col_idx], axis_y - 4),
                    headers.get(col, col),
                    fill="#6b7280",
                    font=small_font,
                )
        palette = ["#2563eb", "#059669", "#d97706", "#7c3aed", "#0891b2"]
        for idx, row in enumerate(rows):
            y = top + idx * row_h
            value = float(row[value_key])
            label = clip(str(row[label_key]), 42)
            draw.text((label_x, y + 3), label, fill="#111827", font=mono_font)
            sublabel = row.get("sublabel")
            if sublabel:
                draw.text((label_x, y + 25), clip(str(sublabel), 54), fill="#6b7280", font=small_font)
            bar_w = int(bar_w_max * (value / max_value))
            color = palette[idx % len(palette)]
            draw.rectangle((bar_x, y, bar_x + bar_w, y + 24), fill=color)
            if not extra_columns:
                draw.text((bar_x + bar_w + 10, y + 3), ms(value), fill="#111827", font=small_font)
            for col_idx, col in enumerate(extra_columns):
                draw.text((col_x[col_idx], y + 3), str(row.get(col, "")), fill="#374151", font=small_font)
        save(image, name, kind)

    image, draw = new_canvas(
        "Stage Timeline",
        "Wall-clock stage spans with parent/child indentation. Long bars are where the publish waited.",
    )
    stages = sorted(run.stages, key=lambda s: s.started_at_ns)
    stages = [s for s in stages if s.duration_ms >= 0.5][:20]
    if stages:
        start_ns = min(s.started_at_ns for s in stages)
        end_ns = max(s.started_at_ns + s.duration_ns for s in stages)
        span_ns = max(1, end_ns - start_ns)
        left = 430
        top = 136
        width = 900
        row_h = 38
        draw_axis(draw, left=left, top=110, width=width, max_value=span_ns / 1_000_000)
        parent_by_stage = {s.name: s.parent for s in stages}
        def depth_for(stage_name: str) -> int:
            depth = 0
            current = parent_by_stage.get(stage_name)
            seen: set[str] = set()
            while current and current not in seen:
                seen.add(current)
                depth += 1
                current = parent_by_stage.get(current)
            return min(depth, 3)
        palette = ["#111827", "#2563eb", "#059669", "#d97706"]
        for idx, stage in enumerate(stages):
            y = top + idx * row_h
            depth = depth_for(stage.name)
            label = clip(stage.name, 46 - depth * 2)
            x0 = left + int(width * ((stage.started_at_ns - start_ns) / span_ns))
            bar_w = max(2, int(width * (stage.duration_ns / span_ns)))
            draw.text((44 + depth * 20, y - 1), label, fill="#111827", font=mono_font)
            draw.rectangle(
                (x0, y, min(left + width, x0 + bar_w), y + 18),
                fill=palette[depth],
            )
            draw.text((1360, y - 1), ms(stage.duration_ms), fill="#374151", font=small_font)
    else:
        empty(draw, "No stage timings recorded.")
    save(image, "phase_timeline.png", "phase-timeline")

    image, draw = new_canvas(
        "Stage Bottlenecks",
        "Inclusive DB time inside each stage vs remaining wall time.",
    )
    stage_rows = analysis["top_stage_rows"][:10]
    if stage_rows:
        max_wall = max(row["wall_ms"] for row in stage_rows) or 1.0
        label_x = 44
        left = 430
        top = 136
        width = 760
        row_h = 62
        draw_axis(draw, left=left, top=110, width=width, max_value=max_wall)
        draw.text((1220, 106), "wall", fill="#6b7280", font=small_font)
        draw.text((1300, 106), "db", fill="#6b7280", font=small_font)
        draw.text((1380, 106), "other", fill="#6b7280", font=small_font)
        for idx, row in enumerate(stage_rows):
            y = top + idx * row_h
            label = clip(row["stage"], 46)
            wall = row["wall_ms"]
            db = min(row["inclusive_db_ms"], wall)
            non_db = max(0.0, wall - db)
            wall_w = int(width * (wall / max_wall))
            db_w = int(wall_w * (db / wall)) if wall > 0 else 0
            draw.text((label_x, y + 2), label, fill="#111827", font=mono_font)
            draw.rectangle((left, y, left + db_w, y + 20), fill="#dc2626")
            draw.rectangle((left + db_w, y, left + wall_w, y + 20), fill="#2563eb")
            draw.text((1220, y + 2), ms(wall), fill="#111827", font=small_font)
            draw.text((1300, y + 2), ms(db), fill="#111827", font=small_font)
            draw.text((1380, y + 2), ms(non_db), fill="#111827", font=small_font)
        draw.rectangle((44, 900, 62, 914), fill="#dc2626")
        draw.text((70, 897), "DB round trip", fill="#374151", font=small_font)
        draw.rectangle((210, 900, 228, 914), fill="#2563eb")
        draw.text((236, 897), "non-DB / orchestration", fill="#374151", font=small_font)
    else:
        empty(draw, "No stage timings recorded.")
    save(image, "stage_bottlenecks.png", "stage-bottlenecks")

    summary = run.summary
    metric_bar_chart(
        "Postgres Round Trip vs Outside Postgres",
        [
            {"label": "postgres round trip", "value": summary.db_round_trip_ms if summary else 0.0},
            {"label": "outside postgres", "value": summary.non_db_ms if summary else 0.0},
        ],
        "db_vs_python.png",
        "db-vs-python",
        "Use this as the first split: DB-bound, Python-bound, or external wait.",
        extra_columns=(),
    )

    metric_bar_chart(
        "Top Query Fingerprints",
        [
            {
                "label": q["fingerprint"],
                "sublabel": q["stage"],
                "value": q["total_ms"],
                "total": ms(q["total_ms"]),
                "calls": f"{q['calls']:,} calls",
                "avg": f"avg {ms(q['avg_ms'])}",
            }
            for q in analysis["top_queries"][:10]
        ],
        "top_queries.png",
        "top-queries",
        "Total DB round-trip time by normalized SQL shape.",
        extra_columns=("total", "calls", "avg"),
    )

    metric_bar_chart(
        "Batching Candidates",
        [
            {
                "label": q["fingerprint"],
                "sublabel": q["stage"],
                "value": q["total_ms"],
                "total": ms(q["total_ms"]),
                "calls": f"{q['calls']:,} calls",
                "avg": f"avg {ms(q['avg_ms'])}",
            }
            for q in analysis["many_small_queries"][:10]
        ],
        "query_churn.png",
        "query-churn",
        "High call-count fingerprints where batching should be considered first.",
        extra_columns=("total", "calls", "avg"),
    )

    image, draw = new_canvas("Collector Evidence", "Unavailable panels are explicit, not fake data.")
    waits = run.evidence.get("waits")
    evidence_rows = [
        ("timing", run.evidence.get("timing")),
        ("postgres_stats", run.evidence.get("postgres_stats")),
        ("waits", waits),
        (
            "rootless_speedscope",
            run.evidence.get("rootless_speedscope")
            or run.evidence.get("python_flamegraph"),
        ),
        ("postgres_flamegraph", run.evidence.get("postgres_flamegraph")),
        ("explain_samples", run.evidence.get("explain_samples")),
    ]
    colors = {"available": "#059669", "degraded": "#d97706", "unavailable": "#6b7280"}
    top = 90
    for idx, (name, ev) in enumerate(evidence_rows):
        y = top + idx * 62
        status = getattr(ev, "status", None) or "unavailable"
        reason = getattr(ev, "reason", None) or ""
        draw.rectangle((44, y, 180, y + 30), fill=colors.get(status, "#6b7280"))
        draw.text((58, y + 7), status, fill="white", font=small_font)
        draw.text((220, y + 5), name, fill="#111827", font=mono_font)
        draw.text((500, y + 5), reason, fill="#6b7280", font=label_font)
    save(image, "evidence.png", "collector-evidence")


def _analyze_profile(run: ProfileRun) -> dict[str, Any]:
    stage_wall: dict[str, float] = {}
    parent_by_stage: dict[str, str | None] = {}
    for stage in run.stages:
        stage_wall[stage.name] = stage_wall.get(stage.name, 0.0) + stage.duration_ms
        parent_by_stage.setdefault(stage.name, stage.parent)

    direct_db_by_stage: dict[str, float] = {}
    inclusive_db_by_stage: dict[str, float] = {}
    query_groups: dict[str, dict[str, Any]] = {}
    for query in run.queries:
        ms = query.duration_ms
        stage = query.stage or "(no stage)"
        direct_db_by_stage[stage] = direct_db_by_stage.get(stage, 0.0) + ms

        current: str | None = stage
        seen: set[str] = set()
        while current and current not in seen:
            seen.add(current)
            inclusive_db_by_stage[current] = inclusive_db_by_stage.get(current, 0.0) + ms
            current = parent_by_stage.get(current)

        group = query_groups.setdefault(query.fingerprint, {
            "fingerprint": query.fingerprint,
            "total_ms": 0.0,
            "calls": 0,
            "rowcount": 0,
            "stages": {},
            "sql": query.sql_shape or "",
        })
        group["total_ms"] += ms
        group["calls"] += 1
        group["rowcount"] += query.rowcount or 0
        stages = group["stages"]
        stages[stage] = stages.get(stage, 0.0) + ms
        if not group["sql"] and query.sql_shape:
            group["sql"] = query.sql_shape

    top_stage_rows = []
    for name, wall_ms in sorted(
        stage_wall.items(), key=lambda item: item[1], reverse=True,
    ):
        inclusive_db = inclusive_db_by_stage.get(name, 0.0)
        top_stage_rows.append({
            "stage": name,
            "wall_ms": wall_ms,
            "direct_db_ms": direct_db_by_stage.get(name, 0.0),
            "inclusive_db_ms": inclusive_db,
            "non_db_ms": max(0.0, wall_ms - min(wall_ms, inclusive_db)),
        })

    query_rows = []
    for group in query_groups.values():
        stages = group["stages"]
        stage = max(stages, key=stages.get) if stages else "(no stage)"
        calls = group["calls"] or 1
        query_rows.append({
            "fingerprint": group["fingerprint"],
            "total_ms": group["total_ms"],
            "calls": group["calls"],
            "avg_ms": group["total_ms"] / calls,
            "rowcount": group["rowcount"],
            "stage": stage,
            "sql": group["sql"],
        })
    top_queries = sorted(
        query_rows, key=lambda row: row["total_ms"], reverse=True,
    )
    many_small_queries = sorted(
        [row for row in query_rows if row["calls"] > 5],
        key=lambda row: (row["total_ms"], row["calls"]),
        reverse=True,
    )
    return {
        "top_stage_rows": top_stage_rows,
        "top_queries": top_queries,
        "many_small_queries": many_small_queries,
        "direct_db_by_stage": direct_db_by_stage,
        "inclusive_db_by_stage": inclusive_db_by_stage,
    }


def _bottleneck_markdown(run: ProfileRun) -> str:
    analysis = _analyze_profile(run)
    summary = run.summary
    lines = [
        f"# Bottleneck Report {run.profile_run_id}",
        "",
        "## Diagnosis",
    ]
    if summary:
        lines.extend([
            f"- Bottleneck class: `{summary.bottleneck_class}`",
            f"- Total duration: `{run.duration_ms or 0:.2f} ms`",
            f"- DB round trip: `{summary.db_round_trip_ms:.2f} ms`",
            f"- Non-DB time: `{summary.non_db_ms:.2f} ms`",
            f"- Top stage: `{summary.top_stage}` ({summary.top_stage_ms:.2f} ms)",
            f"- Top query fingerprint: `{summary.top_query}`",
        ])
    lines.extend([
        "",
        "## Stage Attribution",
        "",
        "| Stage | Wall ms | Inclusive DB ms | Non-DB ms |",
        "|---|---:|---:|---:|",
    ])
    for row in analysis["top_stage_rows"][:12]:
        lines.append(
            f"| `{row['stage']}` | {row['wall_ms']:.2f} | "
            f"{row['inclusive_db_ms']:.2f} | {row['non_db_ms']:.2f} |",
        )

    lines.extend([
        "",
        "## Top Query Fingerprints",
        "",
        "| Fingerprint | Total ms | Calls | Avg ms | Stage | SQL shape |",
        "|---|---:|---:|---:|---|---|",
    ])
    for row in analysis["top_queries"][:12]:
        sql = _clip_sql(row["sql"], 160).replace("|", "\\|")
        lines.append(
            f"| `{row['fingerprint']}` | {row['total_ms']:.2f} | "
            f"{row['calls']} | {row['avg_ms']:.2f} | "
            f"`{row['stage']}` | `{sql}` |",
        )

    lines.extend([
        "",
        "## Many-Small-Query Candidates",
        "",
        "| Fingerprint | Total ms | Calls | Avg ms | Stage |",
        "|---|---:|---:|---:|---|",
    ])
    for row in analysis["many_small_queries"][:12]:
        lines.append(
            f"| `{row['fingerprint']}` | {row['total_ms']:.2f} | "
            f"{row['calls']} | {row['avg_ms']:.2f} | `{row['stage']}` |",
        )
    return "\n".join(lines) + "\n"


def _bottleneck_html(run: ProfileRun) -> str:
    analysis = _analyze_profile(run)
    summary = run.summary
    top_stage = summary.top_stage if summary else None
    bottleneck = summary.bottleneck_class if summary else "unknown"
    db_ms = summary.db_round_trip_ms if summary else 0.0
    non_db_ms = summary.non_db_ms if summary else 0.0
    duration = run.duration_ms or 0.0

    def fmt(value: float) -> str:
        return f"{value:,.1f}"

    def stage_rows() -> str:
        rows = []
        for row in analysis["top_stage_rows"][:14]:
            rows.append(
                "<tr>"
                f"<td><code>{escape(row['stage'])}</code></td>"
                f"<td>{fmt(row['wall_ms'])}</td>"
                f"<td>{fmt(row['inclusive_db_ms'])}</td>"
                f"<td>{fmt(row['non_db_ms'])}</td>"
                f"<td>{_bar_html(row['inclusive_db_ms'], row['wall_ms'])}</td>"
                "</tr>",
            )
        return "\n".join(rows)

    def query_rows(rows: list[dict[str, Any]]) -> str:
        out = []
        for row in rows[:14]:
            out.append(
                "<tr>"
                f"<td><code>{escape(row['fingerprint'])}</code></td>"
                f"<td>{fmt(row['total_ms'])}</td>"
                f"<td>{row['calls']:,}</td>"
                f"<td>{fmt(row['avg_ms'])}</td>"
                f"<td><code>{escape(row['stage'])}</code></td>"
                f"<td class=\"sql\"><code>{escape(_clip_sql(row['sql'], 260))}</code></td>"
                "</tr>",
            )
        return "\n".join(out)

    evidence = "".join(
        "<tr>"
        f"<td><code>{escape(name)}</code></td>"
        f"<td><span class=\"badge {escape(status)}\">{escape(status)}</span></td>"
        f"<td>{escape(reason)}</td>"
        "</tr>"
        for name, ev in sorted(run.evidence.items())
        for status, reason in [(ev.status, ev.reason or "")]
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>OKG Profile {escape(run.profile_run_id)}</title>
  <style>
    :root {{
      color-scheme: light;
      --fg: #111827;
      --muted: #6b7280;
      --line: #e5e7eb;
      --panel: #f9fafb;
      --db: #dc2626;
      --other: #2563eb;
      --ok: #059669;
      --warn: #d97706;
    }}
    body {{
      margin: 0;
      padding: 32px;
      font: 13px/1.45 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      color: var(--fg);
      background: white;
    }}
    h1, h2 {{ margin: 0 0 12px; font-size: 18px; }}
    h2 {{ margin-top: 32px; font-size: 15px; }}
    .meta {{ color: var(--muted); margin-bottom: 22px; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 24px;
    }}
    .metric {{
      border: 1px solid var(--line);
      background: var(--panel);
      padding: 14px;
      border-radius: 6px;
    }}
    .metric .label {{ color: var(--muted); font-size: 11px; }}
    .metric .value {{ font-size: 20px; margin-top: 8px; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 8px; }}
    th, td {{ border-bottom: 1px solid var(--line); padding: 8px 10px; vertical-align: top; }}
    th {{ text-align: left; color: var(--muted); font-weight: 600; }}
    td:nth-child(2), td:nth-child(3), td:nth-child(4) {{ text-align: right; white-space: nowrap; }}
    code {{ font-family: inherit; }}
    .sql {{ color: #374151; }}
    .bar {{ width: 180px; height: 12px; background: #dbeafe; display: inline-flex; }}
    .bar .db {{ background: var(--db); height: 12px; }}
    .bar .other {{ background: var(--other); height: 12px; }}
    .badge {{ color: white; border-radius: 999px; padding: 2px 8px; }}
    .available {{ background: var(--ok); }}
    .degraded {{ background: var(--warn); }}
    .unavailable {{ background: var(--muted); }}
  </style>
</head>
<body>
  <h1>OKG Bottleneck Report</h1>
  <div class="meta">
    profile <code>{escape(run.profile_run_id)}</code> · deployment
    <code>{escape(run.deployment)}</code> · target <code>{escape(run.target_kind)}</code>
    · generation <code>{escape(str(run.generation_id))}</code>
  </div>
  <section class="grid">
    <div class="metric"><div class="label">Bottleneck</div><div class="value">{escape(bottleneck)}</div></div>
    <div class="metric"><div class="label">Duration</div><div class="value">{fmt(duration)} ms</div></div>
    <div class="metric"><div class="label">DB round trip</div><div class="value">{fmt(db_ms)} ms</div></div>
    <div class="metric"><div class="label">Non-DB</div><div class="value">{fmt(non_db_ms)} ms</div></div>
  </section>
  <h2>Diagnosis</h2>
  <p>
    The top wall-clock stage is <code>{escape(str(top_stage))}</code>.
    DB round trips account for {fmt(db_ms)} ms of {fmt(duration)} ms,
    so this run should be optimized from SQL plans, gate queries, indexes,
    and query batching before Python orchestration.
  </p>
  <h2>Stage Attribution</h2>
  <table>
    <thead><tr><th>Stage</th><th>Wall ms</th><th>Inclusive DB ms</th><th>Non-DB ms</th><th>Split</th></tr></thead>
    <tbody>{stage_rows()}</tbody>
  </table>
  <h2>Top Query Fingerprints</h2>
  <table>
    <thead><tr><th>Fingerprint</th><th>Total ms</th><th>Calls</th><th>Avg ms</th><th>Stage</th><th>SQL shape</th></tr></thead>
    <tbody>{query_rows(analysis["top_queries"])}</tbody>
  </table>
  <h2>Many-Small-Query Candidates</h2>
  <table>
    <thead><tr><th>Fingerprint</th><th>Total ms</th><th>Calls</th><th>Avg ms</th><th>Stage</th><th>SQL shape</th></tr></thead>
    <tbody>{query_rows(analysis["many_small_queries"])}</tbody>
  </table>
  <h2>Collector Evidence</h2>
  <table>
    <thead><tr><th>Collector</th><th>Status</th><th>Reason</th></tr></thead>
    <tbody>{evidence}</tbody>
  </table>
</body>
</html>
"""


def _bar_html(db_ms: float, wall_ms: float) -> str:
    wall = max(wall_ms, 0.001)
    db_pct = max(0.0, min(100.0, 100.0 * db_ms / wall))
    other_pct = max(0.0, 100.0 - db_pct)
    return (
        "<span class=\"bar\">"
        f"<span class=\"db\" style=\"width:{db_pct:.1f}%\"></span>"
        f"<span class=\"other\" style=\"width:{other_pct:.1f}%\"></span>"
        "</span>"
    )


def _clip_sql(sql: str, limit: int) -> str:
    sql = re.sub(r"\s+", " ", sql).strip()
    if len(sql) <= limit:
        return sql
    return f"{sql[:limit - 3]}..."


def _stage_wall(analysis: dict[str, Any], stage_name: str) -> float:
    for row in analysis.get("top_stage_rows", []):
        if row.get("stage") == stage_name:
            return float(row.get("wall_ms") or 0.0)
    return 0.0


def _enforce_retention(deployment: str, policy: dict[str, Any]) -> None:
    keep = (
        policy.get("deep_capture", {}).get("max_bundles")
        if isinstance(policy, dict)
        else None
    )
    try:
        keep_int = int(keep)
    except Exception:  # noqa: BLE001 — policy.deep_capture.max_bundles may be None / non-numeric; default to 20.
        keep_int = 20
    if keep_int <= 0:
        return
    root = deployment_root(deployment)
    bundles = [
        p for p in root.iterdir()
        if p.is_dir() and p.name != "_live" and (p / "profile.json").is_file()
    ]
    bundles.sort(key=lambda p: (p / "profile.json").stat().st_mtime, reverse=True)
    for stale in bundles[keep_int:]:
        shutil.rmtree(stale, ignore_errors=True)


def _trace_events(run: ProfileRun) -> dict[str, Any]:
    events: list[dict[str, Any]] = []
    for s in run.stages:
        events.append({
            "name": s.name,
            "cat": "stage",
            "ph": "X",
            "ts": s.started_at_ns / 1000,
            "dur": s.duration_ns / 1000,
            "pid": os.getpid(),
            "tid": 0,
            "args": s.attrs,
        })
    for q in run.queries:
        if q.started_at_ns is None:
            continue
        events.append({
            "name": q.fingerprint,
            "cat": "query",
            "ph": "X",
            "ts": q.started_at_ns / 1000,
            "dur": q.duration_ns / 1000,
            "pid": os.getpid(),
            "tid": 1,
            "args": {
                "stage": q.stage,
                "rowcount": q.rowcount,
                "sql_shape": q.sql_shape,
            },
        })
    return {"traceEvents": events, "displayTimeUnit": "ms"}


def _summary_markdown(run: ProfileRun) -> str:
    s = run.summary
    lines = [
        f"# Profile {run.profile_run_id}",
        "",
        f"- Deployment: `{run.deployment}`",
        f"- Target: `{run.target_kind}`",
        f"- Command: `{run.command}`",
        f"- Started: `{run.started_at}`",
        f"- Duration: `{run.duration_ms or 0:.2f} ms`",
    ]
    if s:
        lines.extend([
            f"- Bottleneck: `{s.bottleneck_class}`",
            f"- Top stage: `{s.top_stage}` ({s.top_stage_ms:.2f} ms)",
            f"- DB round trip: `{s.db_round_trip_ms:.2f} ms`",
        ])
    plot_artifacts = [
        a for a in run.artifacts
        if a.media_type.startswith("image/") and a.path.startswith("plots/")
    ]
    if plot_artifacts:
        lines.append("")
        lines.append("## Plots")
        for artifact in plot_artifacts:
            lines.append(f"- [{artifact.kind}]({artifact.path})")
    if run.limitations:
        lines.append("")
        lines.append("## Limitations")
        for lim in run.limitations:
            lines.append(f"- {lim}")
    return "\n".join(lines) + "\n"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
