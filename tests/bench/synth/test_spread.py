"""task.var.aggregator witnesses: the multi-draw spread aggregator (req.var.aggregator).

Artifact-only and DB-free (no network, no Postgres): every witness synthesizes
recorded artifact directories (manifest + engine-reads + run-binding) at the
CURRENT scorecard schema and drives ``aggregate_draws`` over them. Each draw dir
is a real, rescorable artifact set, so the aggregator's own contract — rebuild via
``rescore_from_artifacts``, refuse the non-distributions, reduce to min/mean/max —
is exercised end to end without a model call.

The witnesses the PACT demands, each shown to FIRE:

* mixed-digest refusal, naming BOTH digests (two draws of genuinely different
  configs — one smoke, one seed-variant);
* N=1 refusal (a lone draw reports itself) and N=0 refusal;
* purity — aggregating the same dirs twice is byte-identical, and the result is
  independent of the argument order;
* a synthesized THREE-dir fixture with small controlled differences across draws,
  aggregated with hand-computed min/mean/max asserted per block.

The cross-seed half (task.row.statistics) adds the second question and keeps it
visibly separate: a spread over draws differing ONLY in the seed, refusals for a
change beyond the seed and for a repeated seed, artifacts whose ``kind`` and
``question`` distinguish the two, and the seed-paired arm effect — including a
fixture where the effect does NOT separate, so the verdict is something a result
can fail.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.bench.synth.config import load_config, load_preset
from okg.bench.synth.corruption import Planner
from okg.bench.synth.fhir_map import world_from_synthea_dir
from okg.bench.synth.goldseal import build_manifest, write_manifest
from okg.bench.synth.prf import SynthPrf
from okg.bench.synth.render import render_corpus
from okg.bench.synth.resolution import BucketResolution, corruption_bucket_map, synth_slug
from okg.bench.synth.runner import (
    MANIFEST_FILENAME,
    RUN_BINDING_FILENAME,
    _subset_world,
    _write_engine_reads,
    build_synth_scorecard,
    rescore_from_artifacts,
)
from okg.bench.synth.spread import (
    CROSS_SEED_KIND,
    CROSS_SEED_SCHEMA,
    SPREAD_SCHEMA,
    WITHIN_SEED_KIND,
    SpreadError,
    aggregate_cross_seed,
    aggregate_draws,
    paired_arm_effect,
    render_cross_seed,
    render_paired_effect,
    render_spread,
    seed_invariant_digest,
    write_spread,
)
from okg.bench.injection.scoring import FindingView, PipelineState

_SYNTH = Path(__file__).resolve().parents[3] / "src/okg/bench/synth"


# --------------------------------------------------------------------------- #
# manifest + claim-set construction (the same offline pattern as test_survival) #
# --------------------------------------------------------------------------- #
def _manifest_for(config):
    world_dir = _SYNTH / "world_smoke"
    prov = json.loads((world_dir / "provenance.json").read_text())
    world = world_from_synthea_dir(
        world_dir, reference_date=prov["reference_date"], provenance=prov
    )
    world = _subset_world(world, config.scale.patients)
    prf = SynthPrf(config.seed)
    planner = Planner(world, config, prf)
    plan = planner.plan()
    rendered = render_corpus(world, config, planner, plan, prf)
    return build_manifest(config=config, world=world, plan=plan, rendered=rendered)


def _smoke_manifest():
    config = load_preset("smoke")
    return _manifest_for(config), config


def _variant_manifest():
    """A genuinely different config (a changed seed → a different config digest)."""
    base = load_preset("smoke")
    variant = load_config({**base.model_dump(mode="json"), "seed": "spread-variant-seed"})
    return _manifest_for(variant), variant


def _lossless_claims(manifest) -> dict[str, str]:
    patients = manifest["patients"]
    claims: dict[str, str] = {}
    for row in manifest["trace"]:
        entity = patients[row["fact_id"].split(":")[1]]["entity"]
        pred = synth_slug(row["predicate_label"])
        claims[f"claim:{row['doc']}:{pred}:{entity}:0"] = row["surface_value"]
    return claims


def _cross_doc_sites(manifest):
    buckets = corruption_bucket_map(manifest)
    return [
        (site, buckets[site["corruption_id"]])
        for site in manifest["gold"]["corruptions"]
        if site["partition"] == "cross_doc"
    ]


def _claim_id(site, bucket) -> str:
    pred, entity = bucket.split(":", 1)
    return f"claim:{site['doc']}:{pred}:{entity}:0"


def _core(manifest, config, claims, *, findings=(), resolutions=()):
    """Build a scorecard exactly as rescore will rebuild it from the written dir."""
    corrupted = PipelineState(
        claims=claims, candidates=frozenset(), findings=tuple(findings)
    )
    clean = PipelineState(claims={}, candidates=frozenset(), findings=())
    return build_synth_scorecard(
        manifest=manifest,
        clean_state=clean,
        corrupted_state=corrupted,
        routed=frozenset(),
        config=config,
        resolutions=tuple(resolutions),
    )


def _write_draw(
    draw_dir: Path,
    manifest,
    *,
    claims,
    findings=(),
    resolutions=(),
    clean_gen="gen-clean",
    corrupted_gen="gen-corrupted",
) -> Path:
    """Write a rescorable artifact dir: manifest + engine-reads + run-binding."""
    draw_dir.mkdir(parents=True, exist_ok=True)
    corrupted = PipelineState(
        claims=claims, candidates=frozenset(), findings=tuple(findings)
    )
    clean = PipelineState(claims={}, candidates=frozenset(), findings=())
    write_manifest(manifest, draw_dir / MANIFEST_FILENAME)
    _write_engine_reads(
        draw_dir,
        clean_state=clean,
        corrupted_state=corrupted,
        routed=frozenset(),
        resolutions=tuple(resolutions),
    )
    (draw_dir / RUN_BINDING_FILENAME).write_text(
        json.dumps(
            {
                "schema": "okg.synth.run-binding.v1",
                "deployment": "synthetic-corpus-smoke",
                "extractor": "llm",
                "clean_generation_id": clean_gen,
                "corrupted_generation_id": corrupted_gen,
            },
            sort_keys=True,
            indent=1,
        )
        + "\n",
        encoding="utf-8",
    )
    return draw_dir


def _reduce(values):
    """The oracle leaf: min/mean/max plus range and sample stdev (n-1).

    Written out independently of the aggregator's own primitives, in the same
    fixed order, so the comparison is an oracle and not a tautology.
    """
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return {
        "min": min(values),
        "mean": mean,
        "max": max(values),
        "range": max(values) - min(values),
        "stdev": variance**0.5,
    }


# --------------------------------------------------------------------------- #
# the refusals (each shown to fire)                                            #
# --------------------------------------------------------------------------- #
def test_n0_refusal() -> None:
    """No draws at all is refused loudly."""
    with pytest.raises(SpreadError) as exc:
        aggregate_draws([])
    assert "N=0" in str(exc.value)


def test_n1_refusal_names_the_digest(tmp_path) -> None:
    """A single draw reports itself; a spread over N=1 is refused, and the refusal
    carries the config digest so the operator sees WHICH lone config it was."""
    manifest, config = _smoke_manifest()
    draw = _write_draw(tmp_path / "draw_a", manifest, claims=_lossless_claims(manifest))
    with pytest.raises(SpreadError) as exc:
        aggregate_draws([draw])
    message = str(exc.value)
    assert "N=1" in message
    assert manifest["config_digest"] in message


def test_mixed_digest_refusal_names_both_digests(tmp_path) -> None:
    """Two draws of DIFFERENT configs are not a distribution: the refusal fires and
    names both digests. The two draws come from genuinely different configs (a
    changed seed), and each draw is rescored to its own config digest first — so the
    refusal is on the rebuilt digest, never the written scorecard's vintage."""
    smoke_manifest, _c = _smoke_manifest()
    variant_manifest, _v = _variant_manifest()
    assert smoke_manifest["config_digest"] != variant_manifest["config_digest"]

    a = _write_draw(tmp_path / "draw_a", smoke_manifest, claims=_lossless_claims(smoke_manifest))
    b = _write_draw(
        tmp_path / "draw_b", variant_manifest, claims=_lossless_claims(variant_manifest)
    )
    with pytest.raises(SpreadError) as exc:
        aggregate_draws([a, b])
    message = str(exc.value)
    assert "mixed config digests" in message
    assert smoke_manifest["config_digest"] in message
    assert variant_manifest["config_digest"] in message


# --------------------------------------------------------------------------- #
# purity                                                                       #
# --------------------------------------------------------------------------- #
def test_double_aggregation_is_byte_identical(tmp_path) -> None:
    """The aggregate is a pure function of the artifact dirs: aggregating the same
    dirs twice yields byte-identical JSON, and re-writing the artifact is
    byte-identical too. No timestamp, no randomness."""
    manifest, _config = _smoke_manifest()
    claims = _lossless_claims(manifest)
    a = _write_draw(tmp_path / "draw_a", manifest, claims=claims, corrupted_gen="gen-a")
    b = _write_draw(tmp_path / "draw_b", manifest, claims=claims, corrupted_gen="gen-b")

    first = json.dumps(aggregate_draws([a, b]), sort_keys=True)
    second = json.dumps(aggregate_draws([a, b]), sort_keys=True)
    assert first == second

    p1 = write_spread(aggregate_draws([a, b]), tmp_path / "spread1.json")
    p2 = write_spread(aggregate_draws([a, b]), tmp_path / "spread2.json")
    assert p1.read_bytes() == p2.read_bytes()
    reloaded = json.loads(p1.read_text())
    assert reloaded["schema"] == SPREAD_SCHEMA and reloaded["n"] == 2


def test_aggregate_is_order_independent(tmp_path) -> None:
    """Purity over the dirs as a SET: the aggregate does not depend on the order the
    dirs are passed in (draws are ordered by resolved path internally)."""
    manifest, _config = _smoke_manifest()
    claims = _lossless_claims(manifest)
    a = _write_draw(tmp_path / "draw_a", manifest, claims=claims, corrupted_gen="gen-a")
    b = _write_draw(tmp_path / "draw_b", manifest, claims=claims, corrupted_gen="gen-b")
    c = _write_draw(tmp_path / "draw_c", manifest, claims=claims, corrupted_gen="gen-c")
    assert aggregate_draws([a, b, c]) == aggregate_draws([c, a, b])


# --------------------------------------------------------------------------- #
# the synthesized three-dir fixture: hand-computed min/mean/max per block       #
# --------------------------------------------------------------------------- #
def test_three_dir_fixture_min_mean_max(tmp_path) -> None:
    """Three draws of the SAME config with small controlled differences:

    * draw_a — lossless extraction; one cross-doc site (S1) is REPAIRED (a
      canonical resolution whose winner is the clean value + a finding);
    * draw_b — S1's corrupted claim is dropped (extraction loss);
    * draw_c — S1's AND a second site's (S2) corrupted claims are dropped.

    Draws are ordered by path, so the per-draw values line up as [a, b, c]. Every
    headline block's min/mean/max is asserted; the cleanly-controlled blocks with
    literal hand-computed values, detection against an independent reduction of the
    three rebuilt scorecards.
    """
    manifest, config = _smoke_manifest()
    denominator = len(manifest["trace"])
    sites = _cross_doc_sites(manifest)
    assert len(sites) >= 2, "the smoke manifest must carry >=2 cross_doc sites"
    (s1, b1), (s2, b2) = sites[0], sites[1]
    cls1, cls2 = s1["corruption_class"], s2["corruption_class"]
    assert cls1 != cls2, "picked two distinct-class cross_doc sites"

    # draw_a: lossless + S1 repaired (canonical resolution to the clean value).
    claims_a = _lossless_claims(manifest)
    pred1, key1 = b1.split(":", 1)
    findings_a = (FindingView(finding_id="f-s1", channel="definite", bucket_key=b1),)
    resolutions_a = (
        BucketResolution(
            predicate=pred1,
            bucket_key=key1,
            outcome="canonical",
            basis="declared_source_authority",
            winner_claim_id=_claim_id(s1, b1),
            ranked_claim_ids=(_claim_id(s1, b1),),
            winner_value=s1["pre_text"],  # the CLEAN value -> repaired
        ),
    )

    # draw_b: S1's corrupted rendering never became a claim.
    claims_b = _lossless_claims(manifest)
    del claims_b[_claim_id(s1, b1)]

    # draw_c: S1 and S2 both lost at extraction.
    claims_c = _lossless_claims(manifest)
    del claims_c[_claim_id(s1, b1)]
    del claims_c[_claim_id(s2, b2)]

    a = _write_draw(
        tmp_path / "draw_a", manifest, claims=claims_a, findings=findings_a,
        resolutions=resolutions_a, corrupted_gen="gen-a",
    )
    b = _write_draw(tmp_path / "draw_b", manifest, claims=claims_b, corrupted_gen="gen-b")
    c = _write_draw(tmp_path / "draw_c", manifest, claims=claims_c, corrupted_gen="gen-c")

    # the independent oracle: the three scorecards rescore will rebuild, in [a,b,c].
    core_a = _core(manifest, config, claims_a, findings=findings_a, resolutions=resolutions_a)
    core_b = _core(manifest, config, claims_b)
    core_c = _core(manifest, config, claims_c)
    cores = [core_a, core_b, core_c]

    agg = aggregate_draws([a, b, c])
    assert agg["n"] == 3
    assert agg["config_digest"] == manifest["config_digest"]
    m = agg["metrics"]

    # -- extraction: recovered [308,307,306], recall the derived rates ------------
    recovered = [c["extraction"]["recovered"] for c in cores]
    assert recovered == [denominator, denominator - 1, denominator - 2]
    assert m["extraction"]["recovered"] == _reduce(
        [denominator, denominator - 1, denominator - 2]
    )
    assert m["extraction"]["recovered"]["mean"] == denominator - 1
    assert m["extraction"]["denominator"] == _reduce([denominator] * 3)
    assert m["extraction"]["denominator"]["range"] == 0
    assert m["extraction"]["denominator"]["stdev"] == 0.0
    recall_vals = [r / denominator for r in recovered]
    assert m["extraction"]["recall"] == _reduce(recall_vals)
    assert m["extraction"]["recall"]["max"] == 1.0
    assert m["extraction"]["recall"]["min"] == (denominator - 2) / denominator

    # -- repair tally: repaired [1,0,0], the rest constant, unadjudicated [10,11,11]
    total = core_a["repair"]["total"]
    assert total == core_b["repair"]["total"] == core_c["repair"]["total"]
    assert m["repair"]["repaired"] == _reduce([1, 0, 0])
    assert m["repair"]["repaired"]["mean"] == 1 / 3
    assert m["repair"]["mis_repaired"] == _reduce([0, 0, 0])
    assert m["repair"]["detected_only"] == _reduce([0, 0, 0])
    assert m["repair"]["unadjudicated"] == _reduce([total - 1, total, total])

    # -- per-class survival: S1's class [1,0,0], S2's class [1,1,0], others constant
    assert m["per_class_survival"][cls1]["rate"] == _reduce([1.0, 0.0, 0.0])
    assert m["per_class_survival"][cls2]["rate"] == _reduce([1.0, 1.0, 0.0])
    untouched = sorted(
        set(core_a["per_class_survival"]["by_class"]) - {cls1, cls2}
    )
    assert untouched, "there must be a class untouched by the drops"
    for cls in untouched:
        assert m["per_class_survival"][cls]["rate"] == _reduce([1.0, 1.0, 1.0])

    # -- detection: asserted against the independent reduction of the rebuilt cores
    def _det_rate(core, form):
        cell = core["funnel"]["detection_recall"][form]
        return (cell["detected"] / cell["denominator"]) if cell["denominator"] else 0.0

    assert m["detection"]["conditional"]["rate"] == _reduce(
        [_det_rate(c, "conditional") for c in cores]
    )
    assert m["detection"]["end_to_end"]["rate"] == _reduce(
        [_det_rate(c, "end_to_end") for c in cores]
    )
    e2e_detected = [c["funnel"]["detection_recall"]["end_to_end"]["detected"] for c in cores]
    assert e2e_detected == [1, 0, 0], "only S1 is detected, in draw_a"
    assert m["detection"]["end_to_end"]["detected"] == _reduce([1, 0, 0])

    # -- majority-flavor + poisoning present and zero (a non-provenance, non-poison run)
    # This is an AGGREGATOR shape claim, not an engine claim: the draws are
    # hand-built, so the zeros are given. What it pins is that a run declaring
    # neither axis still carries both blocks with the {min,mean,max} shape —
    # an aggregator that dropped or mis-shaped either would fail here. Both the
    # denominator (preserved) and the count are asserted, so it is not the
    # "zero read from an empty set" pattern.
    for flavor in ("copy", "independent"):
        assert m["majority_flavor"][flavor]["preserved"] == _reduce([0, 0, 0])
        assert m["majority_flavor"][flavor]["mis_repaired"] == _reduce([0, 0, 0])
    assert m["poisoning"]["poisoned"] == _reduce([0, 0, 0])
    assert m["poisoning"]["flipped"] == _reduce([0, 0, 0])
    assert m["poisoning"]["flip_rate"] == _reduce([0.0, 0.0, 0.0])

    # -- the per-draw generation bindings ride along, in sorted-dir order ----------
    assert [d["corrupted_generation_id"] for d in agg["draws"]] == ["gen-a", "gen-b", "gen-c"]


def test_render_spread_names_every_block(tmp_path) -> None:
    """The operator report renders every headline block and reflects the variation
    (extraction recall min < max), so it is non-vacuous in each section."""
    manifest, _config = _smoke_manifest()
    sites = _cross_doc_sites(manifest)
    (s1, b1) = sites[0]
    claims_a = _lossless_claims(manifest)
    claims_b = _lossless_claims(manifest)
    del claims_b[_claim_id(s1, b1)]
    a = _write_draw(tmp_path / "draw_a", manifest, claims=claims_a, corrupted_gen="gen-a")
    b = _write_draw(tmp_path / "draw_b", manifest, claims=claims_b, corrupted_gen="gen-b")

    report = render_spread(aggregate_draws([a, b]))
    for token in (
        "synth spread",
        "extraction recall",
        "detection conditional",
        "detection end-to-end",
        "repair tally states",
        "per-class injection survival",
        "majority-flavor",
        "poisoning",
        "gen-a",
        "gen-b",
    ):
        assert token in report, token


# --------------------------------------------------------------------------- #
# cross-seed spread: the OTHER question, kept visibly apart                    #
# --------------------------------------------------------------------------- #
def _seed_manifest(seed: str, **overrides):
    """A smoke config at a given seed (and optional non-seed overrides)."""
    base = load_preset("smoke").model_dump(mode="json")
    raw = {**base, "seed": seed, **overrides}
    config = load_config(raw)
    return _manifest_for(config), config


def _seed_draw(tmp_path: Path, seed: str, *, drop: int = 0, **overrides) -> Path:
    """One recorded draw at ``seed``, with ``drop`` claims lost at extraction."""
    manifest, _config = _seed_manifest(seed, **overrides)
    claims = _lossless_claims(manifest)
    for key in sorted(claims)[:drop]:
        del claims[key]
    return _write_draw(
        tmp_path / f"draw_{seed}_{drop}_{len(overrides)}",
        manifest,
        claims=claims,
        corrupted_gen=f"gen-{seed}",
    )


def test_seed_invariant_digest_ignores_only_the_seed() -> None:
    """The digest that admits a cross-seed spread moves for any OTHER change."""
    base = load_preset("smoke").model_dump(mode="json")
    a = load_config({**base, "seed": "s-a"}).model_dump(mode="json")
    b = load_config({**base, "seed": "s-b"}).model_dump(mode="json")
    c = load_config({**base, "seed": "s-a", "clean_fp_ceiling": 3}).model_dump(mode="json")
    assert seed_invariant_digest(a) == seed_invariant_digest(b)
    assert seed_invariant_digest(a) != seed_invariant_digest(c)


def test_cross_seed_spread_reduces_three_seeds(tmp_path) -> None:
    """Three seed-only variants aggregate, and the artifact SAYS which question it
    answers: kind, question text, the seeds, and a dispersion statistic per leaf."""
    seeds = ["cs-1", "cs-2", "cs-3"]
    drops = [0, 1, 2]
    dirs = [_seed_draw(tmp_path, s, drop=d) for s, d in zip(seeds, drops)]
    agg = aggregate_cross_seed(dirs)

    assert agg["schema"] == CROSS_SEED_SCHEMA
    assert agg["kind"] == CROSS_SEED_KIND
    assert agg["n"] == 3
    assert agg["seeds"] == sorted(seeds)
    assert "NOT decode noise" in agg["question"]
    assert "only two frozen worlds" in agg["question"]
    assert agg["metrics_omitted"] == []
    # every draw carries its own config digest: three distinct ones, one per seed
    assert len({row["config_digest"] for row in agg["draws"]}) == 3

    # The oracle is built per draw, because a new seed re-draws the corruption
    # plan and the extraction denominator is NOT constant across seeds — which is
    # exactly the variation this spread exists to expose.
    cards = [rescore_from_artifacts(d) for d in sorted(dirs, key=str)]
    recalls = [c["extraction"]["recovered"] / c["extraction"]["denominator"] for c in cards]
    assert len({c["extraction"]["denominator"] for c in cards}) > 1
    assert agg["metrics"]["extraction"]["recall"] == _reduce(recalls)
    assert agg["metrics"]["extraction"]["recall"]["stdev"] > 0.0


def test_cross_seed_refuses_a_change_beyond_the_seed(tmp_path) -> None:
    """The mixed-config refusal is NOT relaxed: a draw that also changed a knob is
    refused, and the refusal names the key that differs."""
    a = _seed_draw(tmp_path, "cs-1")
    b = _seed_draw(tmp_path, "cs-2", clean_fp_ceiling=3)
    with pytest.raises(SpreadError) as exc:
        aggregate_cross_seed([a, b])
    message = str(exc.value)
    assert "MORE than the seed" in message
    assert "clean_fp_ceiling" in message


def test_cross_seed_refuses_a_repeated_seed(tmp_path) -> None:
    """Two draws of ONE seed are decode noise; folding them into a cross-seed
    spread would conflate the two questions, so it is refused by name."""
    a = _seed_draw(tmp_path, "cs-1", drop=0)
    b = _seed_draw(tmp_path, "cs-1", drop=1)
    with pytest.raises(SpreadError) as exc:
        aggregate_cross_seed([a, b])
    message = str(exc.value)
    assert "repeated seed" in message
    assert "aggregate_draws" in message


def test_cross_seed_refuses_n0_and_n1(tmp_path) -> None:
    with pytest.raises(SpreadError) as exc:
        aggregate_cross_seed([])
    assert "N=0" in str(exc.value)
    with pytest.raises(SpreadError) as exc:
        aggregate_cross_seed([_seed_draw(tmp_path, "cs-1")])
    assert "N=1" in str(exc.value)


def test_the_two_spreads_are_distinguishable_in_the_artifact(tmp_path) -> None:
    """The acceptance in one test: a within-seed spread and a cross-seed spread over
    the same instrument carry different kinds and different stated questions, so a
    reader with one artifact never has to infer which it is."""
    manifest, _config = _seed_manifest("cs-1")
    claims = _lossless_claims(manifest)
    lossy = dict(claims)
    del lossy[sorted(lossy)[0]]
    within_a = _write_draw(tmp_path / "w_a", manifest, claims=claims, corrupted_gen="g-a")
    within_b = _write_draw(tmp_path / "w_b", manifest, claims=lossy, corrupted_gen="g-b")

    within = aggregate_draws([within_a, within_b])
    across = aggregate_cross_seed(
        [_seed_draw(tmp_path, "cs-2"), _seed_draw(tmp_path, "cs-3")]
    )
    assert within["kind"] == WITHIN_SEED_KIND
    assert across["kind"] == CROSS_SEED_KIND
    assert within["question"] != across["question"]
    assert within["seed"] == "cs-1"
    assert "decode noise" in within["question"]

    within_report = render_spread(within)
    across_report = render_cross_seed(across)
    assert "KIND: within_seed_decode_noise" in within_report
    assert "seed: cs-1" in within_report
    assert "CROSS-SEED" in across_report
    assert "NOT decode noise on one corpus" in across_report
    assert "cs-2, cs-3" in across_report
    assert "metrics omitted" in across_report
    assert "range" in across_report and "sd" in across_report


# --------------------------------------------------------------------------- #
# the paired effect                                                            #
# --------------------------------------------------------------------------- #
_ARM_A = {"realism": {"render_mode": "prose", "extractor": "llm"}}
_ARM_B = {"realism": {"render_mode": "structured", "extractor": "llm"}}


def _arm_draw(tmp_path: Path, seed: str, arm: dict, tag: str, drop: int) -> Path:
    manifest, _config = _seed_manifest(seed, **arm)
    claims = _lossless_claims(manifest)
    for key in sorted(claims)[:drop]:
        del claims[key]
    return _write_draw(
        tmp_path / f"arm_{tag}_{seed}", manifest, claims=claims, corrupted_gen=f"g-{tag}-{seed}"
    )


def test_paired_effect_differences_within_each_seed(tmp_path) -> None:
    """Three seed-matched pairs: the per-pair delta, the mean, its scatter, and the
    verdict against the tabulated critical value — no point figure without a spread."""
    pairs = []
    drops = {"ps-1": (4, 1), "ps-2": (5, 1), "ps-3": (6, 2)}
    for seed, (drop_a, drop_b) in drops.items():
        pairs.append(
            (
                _arm_draw(tmp_path, seed, _ARM_A, "a", drop_a),
                _arm_draw(tmp_path, seed, _ARM_B, "b", drop_b),
            )
        )
    effect = paired_arm_effect(pairs, arm_names=("prose", "structured"))

    assert effect["n_pairs"] == 3
    assert effect["arms"] == ["prose", "structured"]
    assert effect["varying_config_keys"] == ["realism"]
    denominator = effect["pairs"][0]["prose"]["value"]
    assert 0.0 < denominator <= 1.0
    # every delta is negative: the prose arm dropped strictly more claims
    assert all(row["delta"] < 0 for row in effect["pairs"])
    deltas = [row["delta"] for row in effect["pairs"]]
    assert effect["delta"] == _reduce(deltas)
    assert effect["df"] == 2
    assert effect["t_critical_two_sided_95"] == 4.303
    assert effect["separated_at_95"] is True
    assert abs(effect["t_statistic"]) > 4.303
    assert "n=3 paired seeds" in effect["caveat"]

    report = render_paired_effect(effect)
    for token in ("PAIRED effect", "ps-1", "paired delta", "critical 95%", "caveat"):
        assert token in report, token


def test_paired_effect_refuses_a_mismatched_pair(tmp_path) -> None:
    """A 'pair' whose members ran at different seeds does not difference the
    corruption draw out, so it is not a pair."""
    pairs = [
        (
            _arm_draw(tmp_path, "ps-1", _ARM_A, "a", 4),
            _arm_draw(tmp_path, "ps-2", _ARM_B, "b", 1),
        ),
        (
            _arm_draw(tmp_path, "ps-3", _ARM_A, "a", 4),
            _arm_draw(tmp_path, "ps-3", _ARM_B, "b", 1),
        ),
    ]
    with pytest.raises(SpreadError) as exc:
        paired_arm_effect(pairs)
    assert "is not a pair" in str(exc.value)


def test_paired_effect_refuses_one_pair_and_a_same_config_pair(tmp_path) -> None:
    """Two more refusals shown to fire: a single pair is not a distribution, and a
    'pair' of two identical configs is not two arms."""
    one = [
        (
            _arm_draw(tmp_path, "ps-1", _ARM_A, "a", 4),
            _arm_draw(tmp_path, "ps-1", _ARM_B, "b", 1),
        )
    ]
    with pytest.raises(SpreadError) as exc:
        paired_arm_effect(one)
    assert "at least two pairs" in str(exc.value)

    same = [
        (
            _arm_draw(tmp_path, "ps-1", _ARM_A, "a", 4),
            _arm_draw(tmp_path, "ps-1", _ARM_A, "a2", 1),
        ),
        (
            _arm_draw(tmp_path, "ps-2", _ARM_A, "a", 4),
            _arm_draw(tmp_path, "ps-2", _ARM_A, "a2", 1),
        ),
    ]
    with pytest.raises(SpreadError) as exc:
        paired_arm_effect(same)
    assert "not two arms" in str(exc.value)


def test_paired_effect_can_fail_to_separate(tmp_path) -> None:
    """Non-vacuity of the verdict: deltas that disagree in SIGN across seeds read
    NOT separated. Without this, 'separated' would be a label nothing can fail —
    and the whole point of repeating the arms is that a delta may not survive."""
    drops = {"ns-1": (5, 1), "ns-2": (1, 5), "ns-3": (3, 2)}
    pairs = [
        (
            _arm_draw(tmp_path, seed, _ARM_A, "a", drop_a),
            _arm_draw(tmp_path, seed, _ARM_B, "b", drop_b),
        )
        for seed, (drop_a, drop_b) in drops.items()
    ]
    effect = paired_arm_effect(pairs, arm_names=("prose", "structured"))
    signs = {row["delta"] > 0 for row in effect["pairs"]}
    assert signs == {True, False}, "the fixture must produce deltas of both signs"
    assert effect["separated_at_95"] is False
    assert abs(effect["t_statistic"]) < 4.303
    assert "NOT separated from zero at 95%" in render_paired_effect(effect)
