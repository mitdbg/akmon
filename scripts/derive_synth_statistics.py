#!/usr/bin/env python3
"""Derive the synth benchmark's SAMPLING artifacts from the stored scored records.

Why this exists
---------------
The rendering-delta claim (prose against structured extraction recall) used to be
N=1 per arm, quoted as a point figure. It is now a seed-paired design over three
seeds, with the within-seed decode noise measured separately on a byte-identical
corpus. Those numbers were computed once, inside a process, and never written
anywhere a reader could open them.

This script recomputes all of them from the committed records
(``bench/synth/records``) and stores each as a durable ANALYSIS record. It makes
no model call, opens no database and needs no credentials: every input is a
stored scored bundle, and ``rescore_from_artifacts`` rebuilds each draw's
scorecard from the manifest and the pinned engine reads. A reader in any checkout
runs this and gets byte-identical artifacts back.

    uv run python scripts/derive_synth_statistics.py            # store
    uv run python scripts/derive_synth_statistics.py --dry-run  # print only

Why three paired effects instead of one
---------------------------------------
Seed ``prose-749-1``'s prose arm was decoded THREE times on a byte-identical
corpus, and those three draws span 6.6 recall points. Any single one of them
could be paired against the structured arm, and the choice moves the headline
delta by more than the delta itself — the first draw gives -5.68 points and the
third gives +0.88. Picking one and reporting it would be the same N=1 mistake
one level up, so the paired effect is computed under EVERY choice and all of them
are stored. The verdict is only quotable if it is the same verdict each time.

The artifacts are written with the draw directories named after the records they
came from, so each artifact's own ``draws`` list cites the durable records a
reader would re-open, not a temporary path on the machine that produced it.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT / "src") not in sys.path:  # pragma: no cover - script bootstrap
    sys.path.insert(0, str(_REPO_ROOT / "src"))

from okg.bench.synth.durable import (  # noqa: E402
    DEFAULT_RECORDS_ROOT,
    read_index,
    resolve_bundle,
    store_analysis_artifact,
    unpack_record,
)
from okg.bench.synth.runner import MANIFEST_FILENAME  # noqa: E402
from okg.bench.synth.spread import (  # noqa: E402
    aggregate_cross_seed,
    aggregate_draws,
    paired_arm_effect,
    render_cross_seed,
    render_paired_effect,
    render_spread,
)

#: The comparison this program's headline delta is about.
METRIC = ("extraction", "recall")
ARM_NAMES = ("prose", "structured")

#: Both arms render the SAME world; only the renderer differs. A record drawn
#: from any other world is not a member of this comparison.
WORLD = "world_prose"

SUMMARY_SCHEMA = "okg.synth.rendering-delta-summary.v1"


def _arm_of(config: dict[str, Any]) -> str | None:
    """``prose`` / ``structured`` from the config's realism block, or None."""
    realism = config.get("realism") or {}
    mode = realism.get("render_mode")
    return str(mode) if mode in ARM_NAMES else None


def _draws(records_root: Path, unpacked: Path) -> dict[str, dict[str, Any]]:
    """Unpack every LLM prose/structured record and index it by record id.

    Each record is unpacked into a directory NAMED after the record, so the
    artifacts below cite record ids rather than temporary paths.
    """
    out: dict[str, dict[str, Any]] = {}
    for row in read_index(records_root)["records"]:
        if row.get("kind") == "analysis":
            continue
        rid = row["record_id"]
        dest = unpack_record(resolve_bundle(rid, records_root), unpacked / rid)
        manifest = json.loads((dest / MANIFEST_FILENAME).read_text(encoding="utf-8"))
        config = manifest["config"]
        realism = config.get("realism") or {}
        # The comparison is between two RENDERINGS of one world, extracted by the
        # model. The deterministic smoke record shares neither the world nor the
        # extractor, so it is not a draw of this instrument — excluded by what it
        # IS, not by a name.
        if realism.get("extractor") != "llm" or config.get("world_path") != WORLD:
            continue
        arm = _arm_of(config)
        if arm is None:
            raise SystemExit(
                f"record {rid} is an llm draw of {WORLD} but its render_mode is "
                f"{realism.get('render_mode')!r}, which is neither arm"
            )
        out[rid] = {
            "seed": str(config["seed"]),
            "arm": arm,
            "cost_usd": (row.get("headline", {}).get("extraction_cost") or {}).get(
                "cost_usd"
            ),
        }
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records-root", default=str(DEFAULT_RECORDS_ROOT))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    records_root = Path(args.records_root)

    with tempfile.TemporaryDirectory(prefix="synth-stats-") as tmp:
        unpacked = Path(tmp)
        draws = _draws(records_root, unpacked)
        if not draws:
            raise SystemExit(
                f"no prose/structured records under {records_root}; there is "
                "nothing to derive a rendering delta from"
            )
        # The aggregators read the draw dirs by path. Working from the unpack root
        # means every recorded path IS a record id.
        os.chdir(unpacked)

        by_arm_seed: dict[tuple[str, str], list[str]] = {}
        for rid, meta in draws.items():
            by_arm_seed.setdefault((meta["arm"], meta["seed"]), []).append(rid)
        for key in by_arm_seed:
            by_arm_seed[key].sort()

        seeds = sorted({meta["seed"] for meta in draws.values()})
        structured = {seed: by_arm_seed[("structured", seed)] for seed in seeds}
        prose = {seed: by_arm_seed[("prose", seed)] for seed in seeds}
        for seed in seeds:
            if len(structured[seed]) != 1:
                raise SystemExit(
                    f"seed {seed} has {len(structured[seed])} structured draws; the "
                    "paired design below assumes one control per seed"
                )

        # The seed whose prose arm was re-decoded: the within-seed noise measurement.
        repeated = sorted(seed for seed in seeds if len(prose[seed]) > 1)
        if len(repeated) != 1:
            raise SystemExit(
                f"expected exactly one seed with repeated prose draws, found {repeated}"
            )
        repeat_seed = repeated[0]

        stored: list[tuple[str, Path]] = []

        def store(label: str, artifact: dict[str, Any], rendered: str, note: str) -> None:
            if args.dry_run:
                print(f"\n===== {label} =====\n{rendered}")
                return
            path = store_analysis_artifact(
                artifact,
                label=label,
                rendered=rendered,
                records_root=records_root,
                note=note,
            )
            stored.append((label, path))
            print(f"stored {label}: {path.name}")

        # ---- within-seed decode noise, on a byte-identical corpus ------------ #
        noise = aggregate_draws(prose[repeat_seed])
        store(
            "prose-decode-noise-spread",
            noise,
            render_spread(noise),
            f"{noise['n']} decodes of ONE prose configuration (seed "
            f"{repeat_seed}) on a byte-identical corpus: the error bar the paired "
            "effect below must be read against.",
        )

        # ---- cross-seed variation, one arm at a time ------------------------- #
        cross_structured = aggregate_cross_seed(
            [structured[seed][0] for seed in seeds]
        )
        store(
            "structured-cross-seed-spread",
            cross_structured,
            render_cross_seed(cross_structured),
            f"structured arm across {len(seeds)} seeds; NOT decode noise.",
        )
        for pick in prose[repeat_seed]:
            chosen = [pick if seed == repeat_seed else prose[seed][0] for seed in seeds]
            cross_prose = aggregate_cross_seed(chosen)
            store(
                "prose-cross-seed-spread",
                cross_prose,
                render_cross_seed(cross_prose),
                f"prose arm across {len(seeds)} seeds, taking {pick} as the "
                f"{repeat_seed} draw; NOT decode noise.",
            )

        # ---- the paired effect, under every choice of the repeated draw ------ #
        effects: list[dict[str, Any]] = []
        for pick in prose[repeat_seed]:
            pairs = [
                (
                    pick if seed == repeat_seed else prose[seed][0],
                    structured[seed][0],
                )
                for seed in seeds
            ]
            effect = paired_arm_effect(pairs, arm_names=ARM_NAMES, metric_path=METRIC)
            effects.append(effect)
            store(
                "prose-vs-structured-paired-effect",
                effect,
                render_paired_effect(effect),
                f"seed-paired {ARM_NAMES[0]} minus {ARM_NAMES[1]} over "
                f"{effect['n_pairs']} seeds, taking {pick} as the {repeat_seed} "
                "prose draw.",
            )

        # ---- the summary: the effect stated AGAINST the noise ---------------- #
        noise_leaf = noise["metrics"]["extraction"]["recall"]
        summary = {
            "schema": SUMMARY_SCHEMA,
            "metric": "/".join(METRIC),
            "arms": list(ARM_NAMES),
            "design": (
                f"{len(seeds)} seed-matched pairs (one prose draw and one "
                f"structured draw per seed), plus {noise['n']} repeated decodes of "
                f"seed {repeat_seed}'s prose arm on a byte-identical corpus."
            ),
            "seeds": seeds,
            "within_seed_decode_noise": {
                "seed": repeat_seed,
                "n": noise["n"],
                "range_points": 100.0 * noise_leaf["range"],
                "stdev_points": 100.0 * noise_leaf["stdev"],
                "min_points": 100.0 * noise_leaf["min"],
                "max_points": 100.0 * noise_leaf["max"],
            },
            "paired_effects": [
                {
                    "repeat_draw_taken": note_pick,
                    "n_pairs": effect["n_pairs"],
                    "per_seed_delta_points": {
                        row["seed"]: 100.0 * row["delta"] for row in effect["pairs"]
                    },
                    "mean_delta_points": 100.0 * effect["delta"]["mean"],
                    "stdev_points": 100.0 * effect["delta"]["stdev"],
                    "t_statistic": effect["t_statistic"],
                    "t_critical_two_sided_95": effect["t_critical_two_sided_95"],
                    "separated_at_95": effect["separated_at_95"],
                }
                for note_pick, effect in zip(prose[repeat_seed], effects)
            ],
            "separated_under_every_choice": all(
                effect["separated_at_95"] for effect in effects
            ),
            "separated_under_no_choice": not any(
                effect["separated_at_95"] for effect in effects
            ),
            "cost_usd_by_arm": {
                arm: round(
                    sum(
                        meta["cost_usd"] or 0.0
                        for meta in draws.values()
                        if meta["arm"] == arm
                    ),
                    6,
                )
                for arm in ARM_NAMES
            },
            "cost_usd_by_record": {
                rid: draws[rid]["cost_usd"] for rid in sorted(draws)
            },
            "draws_by_arm_and_seed": {
                f"{arm}/{seed}": ids for (arm, seed), ids in sorted(by_arm_seed.items())
            },
        }
        store(
            "rendering-delta-summary",
            summary,
            _render_summary(summary),
            "the rendering delta stated as an effect against its own measured "
            "noise, derived from the stored records by this script.",
        )

        if not args.dry_run:
            print(f"\n{len(stored)} analysis record(s) stored under {records_root}")
    return 0


def _render_summary(summary: dict[str, Any]) -> str:
    lines = [
        f"synth RENDERING-DELTA summary  (metric {summary['metric']}, "
        f"{summary['arms'][0]} minus {summary['arms'][1]})",
        f"  design: {summary['design']}",
        f"  seeds: {', '.join(summary['seeds'])}",
        "",
        "  WITHIN-SEED DECODE NOISE (byte-identical corpus, seed "
        f"{summary['within_seed_decode_noise']['seed']}, "
        f"n={summary['within_seed_decode_noise']['n']}):",
        f"    min {summary['within_seed_decode_noise']['min_points']:.2f}  "
        f"max {summary['within_seed_decode_noise']['max_points']:.2f}  "
        f"range {summary['within_seed_decode_noise']['range_points']:.2f} points  "
        f"sd {summary['within_seed_decode_noise']['stdev_points']:.2f} points",
        "",
        "  PAIRED EFFECT, once per choice of the repeated prose draw:",
    ]
    for entry in summary["paired_effects"]:
        deltas = "  ".join(
            f"{seed}={value:+.2f}"
            for seed, value in sorted(entry["per_seed_delta_points"].items())
        )
        verdict = (
            "SEPARATED from zero at 95%"
            if entry["separated_at_95"]
            else "NOT separated from zero at 95%"
        )
        lines.append(f"    taking {entry['repeat_draw_taken']}")
        lines.append(f"      per-seed deltas (points): {deltas}")
        lines.append(
            f"      mean {entry['mean_delta_points']:+.2f}  "
            f"sd {entry['stdev_points']:.2f}  "
            f"t {entry['t_statistic']:+.3f} against critical "
            f"{entry['t_critical_two_sided_95']}  -> {verdict}"
        )
    lines.append("")
    if summary["separated_under_no_choice"]:
        lines.append(
            "  RESULT: the delta is NOT separated from zero under ANY choice of "
            "the repeated draw. It is not an effect; it must not be quoted as a "
            "point figure."
        )
    elif summary["separated_under_every_choice"]:
        lines.append(
            "  RESULT: the delta separates from zero under EVERY choice of the "
            "repeated draw."
        )
    else:
        lines.append(
            "  RESULT: the verdict DEPENDS on which repeated draw is taken, so it "
            "is decided by decode noise rather than by the arms. Not quotable."
        )
    lines.append("")
    lines.append("  measured extraction cost:")
    for arm, cost in sorted(summary["cost_usd_by_arm"].items()):
        lines.append(f"    {arm:12s} ${cost:,.4f}")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main())
