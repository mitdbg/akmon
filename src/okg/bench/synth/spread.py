"""Multi-draw spread aggregators over recorded synth artifact dirs (req.var.aggregator).

TWO SPREADS, TWO QUESTIONS — never one number
---------------------------------------------
:func:`aggregate_draws` reduces draws that share ONE config digest. Since the
seed is inside the digest, those draws share the corpus, the corruption
selection and the document assignment: what varies between them is the model's
decoding. That spread answers "how much does this model vary on THIS corpus" —
decode noise.

:func:`aggregate_cross_seed` reduces draws whose configs differ ONLY in the
seed. A new seed re-draws which facts get corrupted and which documents carry
them, so that spread answers a different question: "how much does this result
depend on which draw we took". It is NOT decode noise, and it is NOT variation
across corpora either — the seed re-draws inside ONE frozen world, and this
benchmark has only two frozen worlds. Every cross-seed artifact says so in its
own ``question`` field and in its rendered report, so the distinction is stated
rather than left for a reader to infer.

Both artifacts carry a ``kind`` and a ``question``; each metric leaf carries
``range`` and ``stdev`` beside min/mean/max, so a spread is reported with a
dispersion statistic and not only with its extremes.

:func:`paired_arm_effect` is the comparison the two spreads exist to support: a
seed-paired difference between two arms. Pairing within a seed removes the
corruption draw from the difference, which is why three PAIRS beat six
independent draws for this question.

The artifact-only sibling of :func:`okg.bench.synth.runner.rescore_from_artifacts`
and :func:`report_from_artifacts`: it calls no model and touches no database. It
takes N recorded artifact directories of the SAME config digest and reports
min/mean/max per headline metric — extraction recall, per-class survival,
conditional and end-to-end detection, the repair tally states (including
mis-repair), and the majority-flavor and poisoning rows where present.

Every draw's scorecard is REBUILT with ``rescore_from_artifacts`` rather than read
from the ``synth-scorecard.json`` on disk, so a draw recorded under an older
scorecard schema is scored under the CURRENT one before aggregation — the
aggregate is uniform even when the written vintages differ.

Three refusals, all loud (no silent degradation):

* mixed config digests — draws of different configs are not a distribution; the
  refusal names every distinct digest it found;
* N == 1 — a single draw reports itself, never a degenerate one-point spread;
* N == 0 — no draws at all.

The aggregate is a PURE function of the artifact dirs: draws are ordered by
resolved path, the reduction primitives are deterministic, and the output carries
no timestamp or randomness — so aggregating the same dirs twice is byte-identical.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from okg.bench.synth.provenance import FLAVORS
from okg.bench.synth.runner import (
    MANIFEST_FILENAME,
    RUN_BINDING_FILENAME,
    rescore_from_artifacts,
)

SPREAD_SCHEMA = "okg.synth.spread.v2"
SPREAD_FILENAME = "synth-spread.json"
CROSS_SEED_SCHEMA = "okg.synth.cross-seed-spread.v1"
CROSS_SEED_FILENAME = "synth-cross-seed-spread.json"
PAIRED_EFFECT_SCHEMA = "okg.synth.paired-effect.v1"
PAIRED_EFFECT_FILENAME = "synth-paired-effect.json"

#: The two spreads name themselves, so an artifact can never be mistaken for the
#: other kind by a reader who only has the file.
WITHIN_SEED_KIND = "within_seed_decode_noise"
CROSS_SEED_KIND = "cross_seed_variation"

WITHIN_SEED_QUESTION = (
    "How much does the model vary on ONE corpus? Every draw shares the config "
    "digest, hence the seed, hence the corruption selection and the document "
    "assignment; only the model's decoding differs. This is decode noise."
)
CROSS_SEED_QUESTION = (
    "How much does the result depend on WHICH draw we took? The draws differ "
    "only in the seed, which re-draws the corruption selection and the document "
    "assignment. This is NOT decode noise, and it is NOT variation across "
    "corpora: the seed re-draws inside ONE frozen world, and the benchmark has "
    "only two frozen worlds."
)

#: Student's t, two-sided 95%, at df = n-1. Only the tiny n this benchmark can
#: afford is tabulated; anything larger must not be silently approximated.
_T95_BY_DF: dict[int, float] = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571}


class SpreadError(RuntimeError):
    """A refusal: the inputs are not a spread (mixed digests, N < 2, or N == 0)."""


# -- the reduction primitives ----------------------------------------------


def _mean(values: Sequence[float]) -> float:
    """Deterministic mean: a fixed-order sum divided by the count.

    Order-fixed because IEEE-754 addition is not associative — the caller passes
    values gathered in the dirs' sorted-path order, so a re-run over the same dirs
    sums in the same order and the bytes match.
    """
    return sum(values) / len(values)


def _stdev(values: Sequence[float]) -> float:
    """Sample standard deviation (n-1), summed in the caller's fixed order.

    A spread reported as min/mean/max alone has no dispersion statistic: two
    draw sets with the same extremes and very different concentration read
    identically. n >= 2 is guaranteed by the aggregators' refusals, so the n-1
    denominator is never zero.
    """
    mean = _mean(values)
    return (sum((v - mean) ** 2 for v in values) / (len(values) - 1)) ** 0.5


def _leaf(values: Sequence[float]) -> dict[str, float]:
    """One metric's spread: min/mean/max plus range and sample stdev."""
    return {
        "min": min(values),
        "mean": _mean(values),
        "max": max(values),
        "range": max(values) - min(values),
        "stdev": _stdev(values),
    }


def _rate(numerator: float, denominator: float) -> float:
    """A recall/survival/flip rate; a zero denominator reads 0.0, never a crash."""
    return (numerator / denominator) if denominator else 0.0


# -- per-draw headline scalars ----------------------------------------------


def _draw_scalars(scorecard: Mapping[str, Any]) -> dict[tuple[str, ...], float]:
    """Flatten one CURRENT-schema scorecard into its headline scalar metrics.

    Keys are dotted paths (as tuples) into the spread's ``metrics`` tree; values
    are the per-draw scalar the aggregator reduces. Rates are floats, tallies are
    ints — ``_leaf`` keeps min/max in the input type and makes mean a float.
    """
    ext = scorecard["extraction"]
    detection = scorecard["funnel"]["detection_recall"]
    repair = scorecard["repair"]

    out: dict[tuple[str, ...], float] = {
        ("extraction", "recall"): _rate(ext["recovered"], ext["denominator"]),
        ("extraction", "recovered"): ext["recovered"],
        ("extraction", "denominator"): ext["denominator"],
    }
    for name in ("conditional", "end_to_end"):
        cell = detection[name]
        out[("detection", name, "rate")] = _rate(cell["detected"], cell["denominator"])
        out[("detection", name, "detected")] = cell["detected"]
        out[("detection", name, "denominator")] = cell["denominator"]
    # The repair tally states (mis-repair among them) — the comparable measurement.
    for state in ("repaired", "mis_repaired", "detected_only", "unadjudicated"):
        out[("repair", state)] = repair[state]
    # Per-class injection survival: the class's survived/sites rate, plus the raw
    # counts for audit. Same config digest ⇒ same sealed gold ⇒ same class set.
    for cls, cell in scorecard["per_class_survival"]["by_class"].items():
        out[("per_class_survival", cls, "rate")] = _rate(cell["survived"], cell["sites"])
        out[("per_class_survival", cls, "survived")] = cell["survived"]
        out[("per_class_survival", cls, "sites")] = cell["sites"]
    # Majority-flavor mis-repair, where present (the current schema always emits
    # both flavors; both read 0 on a non-provenance run).
    flavor_block = scorecard.get("majority_flavor") or {}
    for flavor in FLAVORS:
        cell = flavor_block.get(flavor)
        if cell is not None:
            out[("majority_flavor", flavor, "preserved")] = cell["preserved"]
            out[("majority_flavor", flavor, "mis_repaired")] = cell["mis_repaired"]
    # The poisoning flip, where present (flip_rate 0 / poisoned 0 on a non-poison run).
    poisoning = scorecard.get("poisoning")
    if poisoning is not None:
        out[("poisoning", "poisoned")] = poisoning["poisoned"]
        out[("poisoning", "flipped")] = poisoning["flipped"]
        out[("poisoning", "flip_rate")] = poisoning["flip_rate"]
    return out


def _nest(flat: Mapping[tuple[str, ...], Any]) -> dict[str, Any]:
    """Fold dotted-path leaves into the nested ``metrics`` tree."""
    root: dict[str, Any] = {}
    for path, leaf in flat.items():
        node = root
        for part in path[:-1]:
            node = node.setdefault(part, {})
        node[path[-1]] = leaf
    return root


def _draw_config(draw_dir: Path) -> dict[str, Any]:
    """The config the draw was RUN under, read from its sealed manifest.

    The manifest is a required member of every recorded artifact set, so this
    never falls back to a default: a draw whose manifest carries no config is a
    refusal, not a draw with an assumed one.
    """
    manifest_path = Path(draw_dir) / MANIFEST_FILENAME
    if not manifest_path.exists():
        raise SpreadError(
            f"{draw_dir} carries no {MANIFEST_FILENAME}; the seed and the config "
            "it was drawn under cannot be established"
        )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    config = manifest.get("config")
    if not isinstance(config, Mapping) or "seed" not in config:
        raise SpreadError(
            f"{manifest_path} carries no config.seed; a cross-seed spread cannot "
            "be formed from draws whose seeds are unknown"
        )
    return dict(config)


def seed_invariant_digest(config: Mapping[str, Any]) -> str:
    """Digest of a config with the SEED REMOVED.

    Two configs share this digest exactly when they differ in nothing but the
    seed. It is what makes a cross-seed spread expressible without weakening the
    mixed-digest refusal: draws that differ in anything else still have
    different seed-invariant digests and are still refused.
    """
    stripped = {k: v for k, v in config.items() if k != "seed"}
    payload = json.dumps(stripped, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _config_differences(configs: Sequence[Mapping[str, Any]]) -> list[str]:
    """Top-level config keys (seed excluded) that are not identical across draws."""
    keys = sorted({k for c in configs for k in c} - {"seed"})
    out = []
    for key in keys:
        rendered = {
            json.dumps(c.get(key), sort_keys=True, separators=(",", ":"))
            for c in configs
        }
        if len(rendered) > 1:
            out.append(key)
    return out


def _draw_binding(draw_dir: Path) -> dict[str, Any]:
    """The per-draw generation binding recorded for auditability.

    Reads ``run-binding.json`` when present (the real draws always write it): the
    two published generation ids, the deployment, and the extractor. A draw dir
    without one still lists its path.
    """
    row: dict[str, Any] = {"dir": str(draw_dir)}
    binding_path = draw_dir / RUN_BINDING_FILENAME
    if binding_path.exists():
        binding = json.loads(binding_path.read_text(encoding="utf-8"))
        for key in (
            "deployment",
            "extractor",
            "clean_generation_id",
            "corrupted_generation_id",
        ):
            if key in binding:
                row[key] = binding[key]
    return row


# -- the aggregator ---------------------------------------------------------


def aggregate_draws(dirs: Iterable[str | Path]) -> dict[str, Any]:
    """Aggregate N recorded artifact dirs of the SAME config into a spread.

    Rebuilds each draw's scorecard with ``rescore_from_artifacts`` (never trusting
    the written scorecard's schema vintage), refuses mixed digests / N < 2 loudly,
    and returns min/mean/max per headline metric. Pure over the dirs: draws are
    ordered by resolved path, so the result is independent of the argument order
    and byte-identical on a re-run.
    """
    ordered = sorted((Path(d) for d in dirs), key=str)
    if not ordered:
        raise SpreadError(
            "N=0: no artifact directories were given. A spread is a distribution "
            "over draws and needs at least two of the same config."
        )

    scorecards = [rescore_from_artifacts(draw_dir) for draw_dir in ordered]
    digests = sorted({sc["config_digest"] for sc in scorecards})
    if len(digests) > 1:
        raise SpreadError(
            "mixed config digests: draws of different configs are not a "
            f"distribution. Found {len(digests)} distinct digests across "
            f"{len(ordered)} dir(s): {digests}. Aggregate only draws that share "
            "one config digest."
        )
    digest = digests[0]
    if len(ordered) == 1:
        raise SpreadError(
            f"N=1: a single draw reports itself, never a degenerate spread "
            f"(config digest {digest}). Record at least two draws of the same "
            "config before aggregating a spread."
        )

    per_draw = [_draw_scalars(sc) for sc in scorecards]
    key_sets = {frozenset(scalars) for scalars in per_draw}
    if len(key_sets) != 1:
        # Same config digest but a divergent metric surface — refuse rather than
        # silently aggregate whatever the draws happen to share.
        only_some = sorted(
            {"/".join(k) for ks in key_sets for k in ks}
            - {"/".join(k) for k in set.intersection(*map(set, per_draw))}
        )
        raise SpreadError(
            "draws share the config digest but not the metric surface — these "
            "are not uniform draws of one instrument. Metrics present in some "
            f"draws but not all: {only_some}."
        )

    flat = {
        key: _leaf([scalars[key] for scalars in per_draw])
        for key in sorted(per_draw[0])
    }
    seeds = sorted({_draw_config(draw_dir)["seed"] for draw_dir in ordered})
    return {
        "schema": SPREAD_SCHEMA,
        "kind": WITHIN_SEED_KIND,
        "question": WITHIN_SEED_QUESTION,
        "n": len(ordered),
        "config_digest": digest,
        # One digest means one seed, so this is a scalar in practice; it is
        # written out so a reader never has to unpack a manifest to learn WHICH
        # corpus draw the decode noise was measured on.
        "seed": seeds[0] if len(seeds) == 1 else seeds,
        "metrics": _nest(flat),
        "draws": [_draw_binding(draw_dir) for draw_dir in ordered],
    }


def aggregate_cross_seed(dirs: Iterable[str | Path]) -> dict[str, Any]:
    """Aggregate draws that differ ONLY in the seed into a cross-seed spread.

    The complement of :func:`aggregate_draws`, and deliberately not a relaxation
    of it: the mixed-digest refusal stays exactly as strict for configs that
    differ in anything else, because the seed-invariant digest still separates
    them. What is admitted here is the seed and nothing but the seed.

    Four refusals, all loud:

    * N == 0 / N == 1 — a distribution needs draws;
    * configs differing in more than the seed — the refusal names the differing
      config keys, so "you changed the scale too" is visible, not inferred;
    * a REPEATED seed — two draws of one seed are decode noise, and folding them
      in here would silently mix the two questions this module exists to keep
      apart. The refusal points at :func:`aggregate_draws` by name.

    A metric present in some seeds but not all (a corruption class the draw
    never selected, say) is NOT silently averaged over whatever it happens to
    have: the reduction runs over the shared surface and every omitted metric is
    named in ``metrics_omitted``, which the rendered report prints.
    """
    ordered = sorted((Path(d) for d in dirs), key=str)
    if not ordered:
        raise SpreadError(
            "N=0: no artifact directories were given. A cross-seed spread is a "
            "distribution over seeds and needs at least two of them."
        )
    configs = [_draw_config(draw_dir) for draw_dir in ordered]
    invariants = sorted({seed_invariant_digest(c) for c in configs})
    if len(invariants) > 1:
        raise SpreadError(
            "draws differ in MORE than the seed: a cross-seed spread varies the "
            f"seed and nothing else. Found {len(invariants)} distinct "
            f"seed-invariant digests {invariants} across {len(ordered)} dir(s); "
            f"the config keys that differ are {_config_differences(configs)}."
        )
    seeds = [str(c["seed"]) for c in configs]
    if len(set(seeds)) != len(seeds):
        repeated = sorted({s for s in seeds if seeds.count(s) > 1})
        raise SpreadError(
            f"repeated seed(s) {repeated}: two draws of ONE seed measure decode "
            "noise on one corpus, not variation across seeds. Aggregate those "
            "with aggregate_draws (a within-seed spread) and keep the two "
            "questions apart."
        )
    if len(ordered) == 1:
        raise SpreadError(
            f"N=1: a single seed ({seeds[0]}) is not a cross-seed distribution. "
            "Record at least two seeds of the same configuration."
        )

    scorecards = [rescore_from_artifacts(draw_dir) for draw_dir in ordered]
    per_draw = [_draw_scalars(sc) for sc in scorecards]
    shared = set.intersection(*(set(scalars) for scalars in per_draw))
    omitted = sorted(
        {"/".join(k) for scalars in per_draw for k in scalars}
        - {"/".join(k) for k in shared}
    )
    flat = {
        key: _leaf([scalars[key] for scalars in per_draw]) for key in sorted(shared)
    }
    rows = []
    for draw_dir, config, scorecard in zip(ordered, configs, scorecards):
        row = _draw_binding(draw_dir)
        row["seed"] = str(config["seed"])
        row["config_digest"] = scorecard["config_digest"]
        rows.append(row)
    return {
        "schema": CROSS_SEED_SCHEMA,
        "kind": CROSS_SEED_KIND,
        "question": CROSS_SEED_QUESTION,
        "n": len(ordered),
        "seed_invariant_digest": invariants[0],
        "seeds": sorted(seeds),
        "metrics": _nest(flat),
        "metrics_omitted": omitted,
        "draws": rows,
    }


def _metric_value(scorecard: Mapping[str, Any], metric_path: Sequence[str]) -> float:
    """One draw's value of the compared metric, from the REBUILT scorecard."""
    scalars = _draw_scalars(scorecard)
    key = tuple(metric_path)
    if key not in scalars:
        raise SpreadError(
            f"metric {'/'.join(metric_path)} is not a headline scalar of this "
            f"scorecard; available: {sorted('/'.join(k) for k in scalars)}"
        )
    return scalars[key]


def paired_arm_effect(
    pairs: Sequence[tuple[str | Path, str | Path]],
    *,
    arm_names: tuple[str, str] = ("treatment", "control"),
    metric_path: Sequence[str] = ("extraction", "recall"),
) -> dict[str, Any]:
    """The seed-PAIRED difference between two arms, with its own dispersion.

    Each pair is ``(treatment_dir, control_dir)`` run at ONE seed, so the
    corruption selection and the document assignment are common to both members
    and cancel in the difference. That is why three pairs answer the question
    better than three independent draws per side: the seed's contribution is
    differenced out instead of being carried into the noise.

    Refusals: fewer than two pairs; a pair whose two members were run at
    different seeds (not a pair); a pair whose two members share a config digest
    (not two arms); pairs that disagree about WHICH config key distinguishes the
    arms; a seed used by more than one pair.

    Reports the per-pair deltas, their mean, range and sample stdev, and the
    paired t statistic against the tabulated two-sided 95% critical value at
    df = n-1. The verdict is stated as ``separated_at_95``, never implied by a
    point figure — and at these n the interval is wide, which the caveat says.
    """
    if len(pairs) < 2:
        raise SpreadError(
            f"a paired effect needs at least two pairs, got {len(pairs)}. One "
            "pair is a single difference, not a distribution over seeds."
        )
    rows: list[dict[str, Any]] = []
    varying_keys: set[tuple[str, ...]] = set()
    for treatment, control in pairs:
        t_dir, c_dir = Path(treatment), Path(control)
        t_cfg, c_cfg = _draw_config(t_dir), _draw_config(c_dir)
        if str(t_cfg["seed"]) != str(c_cfg["seed"]):
            raise SpreadError(
                f"pair ({t_dir}, {c_dir}) is not a pair: seeds "
                f"{t_cfg['seed']!r} and {c_cfg['seed']!r} differ, so the "
                "corruption draw does not cancel in the difference."
            )
        differing = tuple(_config_differences([t_cfg, c_cfg]))
        if not differing:
            raise SpreadError(
                f"pair ({t_dir}, {c_dir}) is not two arms: the two configs are "
                "identical apart from nothing — same seed, same everything."
            )
        varying_keys.add(differing)
        t_card = rescore_from_artifacts(t_dir)
        c_card = rescore_from_artifacts(c_dir)
        t_value = _metric_value(t_card, metric_path)
        c_value = _metric_value(c_card, metric_path)
        rows.append(
            {
                "seed": str(t_cfg["seed"]),
                arm_names[0]: {
                    "dir": str(t_dir),
                    "config_digest": t_card["config_digest"],
                    "value": t_value,
                },
                arm_names[1]: {
                    "dir": str(c_dir),
                    "config_digest": c_card["config_digest"],
                    "value": c_value,
                },
                "delta": t_value - c_value,
            }
        )
    if len(varying_keys) > 1:
        raise SpreadError(
            "the pairs do not vary the SAME thing: differing config keys per "
            f"pair were {sorted(varying_keys)}. A paired effect over pairs that "
            "change different knobs is not one effect."
        )
    seeds = [row["seed"] for row in rows]
    if len(set(seeds)) != len(seeds):
        repeated = sorted({s for s in seeds if seeds.count(s) > 1})
        raise SpreadError(
            f"seed(s) {repeated} appear in more than one pair; each pair must "
            "contribute an independent seed."
        )

    rows.sort(key=lambda row: row["seed"])
    deltas = [row["delta"] for row in rows]
    n = len(deltas)
    mean = _mean(deltas)
    stdev = _stdev(deltas)
    standard_error = stdev / (n**0.5)
    t_stat = (mean / standard_error) if standard_error else None
    df = n - 1
    critical = _T95_BY_DF.get(df)
    separated = (
        None
        if (t_stat is None or critical is None)
        else bool(abs(t_stat) > critical)
    )
    return {
        "schema": PAIRED_EFFECT_SCHEMA,
        "metric": "/".join(metric_path),
        "arms": list(arm_names),
        "varying_config_keys": sorted(varying_keys.pop()) if varying_keys else [],
        "n_pairs": n,
        "pairs": rows,
        "delta": _leaf(deltas),
        "standard_error": standard_error,
        "t_statistic": t_stat,
        "df": df,
        "t_critical_two_sided_95": critical,
        "separated_at_95": separated,
        "caveat": (
            f"n={n} paired seeds. The 95% interval at df={df} is wide; a "
            "'separated' verdict here means the paired differences agreed in "
            "sign and size relative to their own scatter, not that the effect "
            "size is precisely estimated. Seeds re-draw inside ONE frozen "
            "world, so this generalizes across draws of that world and not "
            "across corpora."
        ),
    }


def write_spread(aggregate: Mapping[str, Any], out_path: str | Path) -> Path:
    """Write the spread artifact (``okg.synth.spread.v1``), sorted-key + deterministic."""
    out_path = Path(out_path)
    out_path.write_text(
        json.dumps(aggregate, sort_keys=True, indent=1) + "\n", encoding="utf-8"
    )
    return out_path


# -- the operator report ----------------------------------------------------


def _num(value: Any) -> str:
    """Format a scalar for the operator report: ints as ints, rates to 4 places."""
    if isinstance(value, bool):  # pragma: no cover - no bool metrics today
        return str(value)
    if isinstance(value, int):
        return str(value)
    return f"{value:.4f}"


def _line(label: str, leaf: Mapping[str, Any]) -> str:
    return (
        f"    {label:30s} min {_num(leaf['min'])}  "
        f"mean {_num(leaf['mean'])}  max {_num(leaf['max'])}  "
        f"range {_num(leaf['range'])}  sd {_num(leaf['stdev'])}"
    )


def render_spread(aggregate: Mapping[str, Any]) -> str:
    """A CLI-free operator report for a spread, in the style of report_from_artifacts.

    Pure over the aggregate — no timestamp, no live read — so the same spread
    renders the same text.
    """
    metrics = aggregate["metrics"]
    lines = [
        f"synth spread  (N={aggregate['n']}, config {aggregate['config_digest'][:16]})",
        f"  KIND: {aggregate.get('kind', WITHIN_SEED_KIND)}"
        f"   seed: {aggregate.get('seed', '(unrecorded)')}",
        f"  WHAT THIS MEASURES: {aggregate.get('question', WITHIN_SEED_QUESTION)}",
        "  draws (per-draw generation bindings):",
    ]
    for row in aggregate["draws"]:
        lines.append(
            f"    {row.get('dir', '?')}"
            f"  clean={row.get('clean_generation_id', '(none)')}"
            f"  corrupted={row.get('corrupted_generation_id', '(none)')}"
        )
    lines.extend(_metric_lines(metrics))
    return "\n".join(lines) + "\n"


def _metric_lines(metrics: Mapping[str, Any]) -> list[str]:
    """The headline metric block, shared by the within-seed and cross-seed reports."""
    lines: list[str] = []
    lines.append("  headline metrics (min / mean / max / range / sd across draws):")
    lines.append(_line("extraction recall", metrics["extraction"]["recall"]))
    detection = metrics["detection"]
    lines.append(_line("detection conditional", detection["conditional"]["rate"]))
    lines.append(_line("detection end-to-end", detection["end_to_end"]["rate"]))
    lines.append("  repair tally states:")
    for state in ("repaired", "mis_repaired", "detected_only", "unadjudicated"):
        lines.append(_line(state, metrics["repair"][state]))
    lines.append("  per-class injection survival (survived/sites rate):")
    for cls, cell in sorted(metrics["per_class_survival"].items()):
        lines.append(_line(cls, cell["rate"]))
    if "majority_flavor" in metrics:
        lines.append("  majority-flavor mis-repair (count over preserved majorities):")
        for flavor, cell in sorted(metrics["majority_flavor"].items()):
            lines.append(_line(f"{flavor} preserved", cell["preserved"]))
            lines.append(_line(f"{flavor} mis_repaired", cell["mis_repaired"]))
    if "poisoning" in metrics:
        poisoning = metrics["poisoning"]
        lines.append("  poisoning flip:")
        lines.append(_line("poisoned", poisoning["poisoned"]))
        lines.append(_line("flipped", poisoning["flipped"]))
        lines.append(_line("flip_rate", poisoning["flip_rate"]))
    return lines


def render_cross_seed(aggregate: Mapping[str, Any]) -> str:
    """The cross-seed report, which says what it is BEFORE it says any number.

    Deliberately not the same header as :func:`render_spread`: a reader holding
    one report must not have to know the other exists to tell which question was
    answered. Pure over the aggregate.
    """
    lines = [
        f"synth CROSS-SEED spread  (N={aggregate['n']} seeds, "
        f"seed-invariant config {aggregate['seed_invariant_digest'][:16]})",
        f"  KIND: {aggregate['kind']}  — NOT decode noise on one corpus.",
        f"  WHAT THIS MEASURES: {aggregate['question']}",
        f"  seeds: {', '.join(aggregate['seeds'])}",
        "  draws (per-seed generation bindings):",
    ]
    for row in aggregate["draws"]:
        lines.append(
            f"    seed={row.get('seed', '?')}"
            f"  config={str(row.get('config_digest', ''))[:16]}"
            f"  clean={row.get('clean_generation_id', '(none)')}"
            f"  corrupted={row.get('corrupted_generation_id', '(none)')}"
        )
    omitted = aggregate.get("metrics_omitted") or []
    lines.append(
        "  metrics omitted (present in some seeds, not all): "
        + (", ".join(omitted) if omitted else "none")
    )
    lines.extend(_metric_lines(aggregate["metrics"]))
    return "\n".join(lines) + "\n"


def render_paired_effect(effect: Mapping[str, Any]) -> str:
    """The paired-effect report: per-seed deltas first, then the effect and its own
    scatter, then the verdict. Pure over the effect."""
    arm_a, arm_b = effect["arms"]
    lines = [
        f"synth PAIRED effect  ({arm_a} minus {arm_b}, metric {effect['metric']})",
        f"  design: {effect['n_pairs']} seed-matched pairs; the arms differ in "
        f"{', '.join(effect['varying_config_keys'])} and nothing else.",
        "  per-seed pairs:",
    ]
    for row in effect["pairs"]:
        lines.append(
            f"    seed={row['seed']:16s} {arm_a}={_num(row[arm_a]['value'])}  "
            f"{arm_b}={_num(row[arm_b]['value'])}  delta={_num(row['delta'])}"
        )
    delta = effect["delta"]
    lines.append(_line("paired delta", delta))
    lines.append(f"    standard error            {_num(effect['standard_error'])}")
    t_stat = effect["t_statistic"]
    critical = effect["t_critical_two_sided_95"]
    lines.append(
        f"    t (df={effect['df']})                  "
        f"{'(undefined: zero scatter)' if t_stat is None else _num(t_stat)}"
        f"   critical 95% {critical if critical is not None else '(untabulated)'}"
    )
    verdict = effect["separated_at_95"]
    lines.append(
        "    verdict                    "
        + (
            "UNDECIDED (no t statistic)"
            if verdict is None
            else ("SEPARATED from zero at 95%" if verdict else "NOT separated from zero at 95%")
        )
    )
    lines.append(f"  caveat: {effect['caveat']}")
    return "\n".join(lines) + "\n"
