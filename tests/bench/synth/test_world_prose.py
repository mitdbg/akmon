"""task.attrib.world witnesses: the frozen prose-scale world and its measured
eligible corruption pools, with world_smoke provably untouched.

Sibling of tests/bench/synth/test_world.py; same fixture idiom and fail-closed
voice. The disjoint-eligible maximum is the tight upper bound on how many
one-per-fact corruptions the planner can place: every corruption class draws
from one of six fact-kind pools that share no fact (cross-class exclusion,
corruption.py ``Planner._pick``), so the pools' sum bounds the total. That sum
is a necessary bound, not the binding one: the planner fails closed PER POOL, so
the witness reads each class's injected count against its own pool, at the scale
the prose_llm preset actually runs (req.attrib.llm-lane). Budget and scale come
from the preset, never from a constant copied into this file.

MOVE-IN NOTE: this file lives in .scratch until world_prose is frozen (the
Synthea jar was unavailable at author time). Drop it into tests/bench/synth/
verbatim once src/okg/bench/synth/world_prose/ exists; it hard-fails (never
skips) if the world is missing.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from okg.bench.synth.config import load_preset
from okg.bench.synth.corruption import Planner, _numeric_facts
from okg.bench.synth.fhir_map import world_from_synthea_dir
from okg.bench.synth.prf import SynthPrf
from okg.bench.synth.runner import _subset_world
from okg.bench.synth.worldschema import GroundTruthWorld

SYNTH_PKG = Path(__file__).resolve().parents[3] / "src/okg/bench/synth"
WORLD_PROSE_DIR = SYNTH_PKG / "world_prose"
WORLD_SMOKE_DIR = SYNTH_PKG / "world_smoke"

# The generator pin (rolling release tag; the jar sha256 is authoritative).
GENERATOR_JAR_SHA256 = "410bc0c41810a5ad194ac654e87ac535ba93a79f4504a8e1bac544c167bfdb8e"

# Which disjoint fact-kind pool each corruption class draws from — the mapping
# corruption.py's per-class candidate filters implement, and the one the
# prose_llm preset's POOL MARGIN arithmetic is written against. The aggregate
# "disjoint max > budget" bound is NOT the binding constraint: the planner fails
# closed per pool, and allergies (one class, 13 of 16 available) is the tight one.
POOL_BY_CLASS = {
    "value_perturbation": "numeric_observations",
    "digit_transposition": "numeric_observations",
    "unit_conversion_error": "numeric_observations",
    "label_value_mismatch": "numeric_observations",
    "entity_swap": "medications",
    "dangling_entity": "medications",
    "derived_value": "derived",
    "broken_reference": "birthdate",
    "cardinality_violation": "care_provider",
    "polarity_flip": "allergies",
}

# Seeds already burned into published corpora (world_smoke; the .scratch 200
# world). The prose world's seed must be a genuinely fresh one.
PUBLISHED_SEEDS = (749202607, 749205000)

# Byte-identity pins for the smoke world DATA and the smoke PRESET this task
# must not disturb. A mismatch is a LOUD regression, never a skip (house rule:
# no silent fallback). The golden scorecard is deliberately NOT pinned here:
# task.attrib.states legitimately evolves it (adds the funnel block), so
# pinning it in this task would be a cross-task landmine; git status is the
# arbiter for the golden.
WORLD_SMOKE_COMBINED_SHA256 = "897ec4976b96d489ad190afa740f224ed94bfd5e2c52f0a5a138eb7877052864"
SMOKE_PRESET_SHA256 = "f71ddd8c36e4f178db2c22aaba0a54e8b3e9ab918463294446e976e5630656dd"


@pytest.fixture(scope="module")
def prose_provenance() -> dict:
    return json.loads((WORLD_PROSE_DIR / "provenance.json").read_text())


@pytest.fixture(scope="module")
def prose_world(prose_provenance: dict) -> GroundTruthWorld:
    return world_from_synthea_dir(
        WORLD_PROSE_DIR,
        reference_date=prose_provenance["reference_date"],
        provenance=prose_provenance,
    )


def _disjoint_pools(world: GroundTruthWorld) -> dict[str, int]:
    """The six disjoint fact-kind pools the ten corruption classes draw from,
    mirroring the per-class candidate filters in corruption.py:

    - ``numeric_observations`` -- ``_numeric_facts(world, "observation:")``,
      the superset for value_perturbation / digit_transposition /
      unit_conversion_error / label_value_mismatch.
    - ``medications`` -- entity_swap / dangling_entity.
    - ``derived`` -- derived_value (facts carrying a functional dependency).
    - ``birthdate`` -- broken_reference.
    - ``care_provider`` -- cardinality_violation.
    - ``allergies`` -- polarity_flip.

    The pools share no fact (distinct fact kinds), so their sum is the
    disjoint-eligible maximum: the most one-per-fact corruptions the planner
    can place under cross-class exclusion.
    """
    return {
        "numeric_observations": len(_numeric_facts(world, "observation:")),
        "medications": len([f for f in world.facts if f.kind.startswith("medication:")]),
        "derived": len([f for f in world.facts if f.derived_from is not None]),
        "birthdate": len([f for f in world.facts if f.kind == "birthdate"]),
        "care_provider": len([f for f in world.facts if f.kind == "care_provider"]),
        "allergies": len([f for f in world.facts if f.kind.startswith("allergy_status:")]),
    }


# -- provenance ------------------------------------------------------------


def test_prose_provenance_mirrors_smoke_fresh_seed(prose_provenance):
    assert prose_provenance["generator"] == "synthea"
    assert prose_provenance["generator_jar_sha256"] == GENERATOR_JAR_SHA256
    assert prose_provenance["reference_date"] == "2026-07-01"
    assert "fresh_seed_attestation" in prose_provenance
    assert prose_provenance["seed"] == prose_provenance["clinician_seed"]
    assert prose_provenance["raw_export_digest"] != prose_provenance["pruned_export_digest"]
    assert prose_provenance["seed"] not in PUBLISHED_SEEDS, (
        "the prose world must carry a genuinely fresh seed, not a republished one"
    )


def test_prose_world_is_prose_scale(prose_world, prose_provenance):
    n = len(prose_world.patients)
    assert prose_provenance["population"] == n, (
        "provenance population must record the ACTUAL frozen count"
    )
    # "roughly 40" -- Synthea emits a few more/fewer living patients than -p.
    assert 30 <= n <= 55, f"prose world holds {n} patients, expected ~40"


# -- the load-bearing pool witness -----------------------------------------


def test_prose_preset_budget_fits_every_pool(capsys):
    """Every corruption class's injected count fits the pool it draws from, in
    THE WORLD THE prose_llm PRESET ACTUALLY RUNS.

    Three things the earlier form got wrong, each of which made it pass while
    the real constraint went unmeasured:

    * it measured the FULL frozen world (42 patients, no fact cap) under a
      default config, not the preset's ``scale`` (24 patients, 40 facts each) —
      a world roughly twice the size of the one the run sees;
    * it compared against a hardcoded 140 that had drifted below the preset's
      own 147 injected corruptions;
    * it bounded the AGGREGATE (sum of all six pools) when the planner fails
      closed PER POOL. The aggregate carries a 1.58x margin while allergies —
      13 requested of 16 available — carries 1.23x, so the aggregate could hold
      with a pool already overrun.

    The budget and the scale are read from the preset, so the preset moving
    without the world moving fails here rather than at run time.
    """
    config = load_preset("prose_llm")
    provenance = json.loads((WORLD_PROSE_DIR / "provenance.json").read_text())
    world = world_from_synthea_dir(
        WORLD_PROSE_DIR,
        reference_date=provenance["reference_date"],
        provenance=provenance,
        max_facts_per_patient=config.scale.max_facts_per_patient,
    )
    world = _subset_world(world, config.scale.patients)
    assert len(world.patients) == config.scale.patients

    planner = Planner(world, config, SynthPrf(config.seed))
    eligibility = planner.eligibility_report()
    assert all(eligibility.values()), f"every class must be expressible: {eligibility}"

    pools = _disjoint_pools(world)
    demand: dict[str, int] = dict.fromkeys(pools, 0)
    for name, spec in config.corruption.classes.items():
        assert name in POOL_BY_CLASS, f"class {name} draws from an unmapped pool"
        demand[POOL_BY_CLASS[name]] += spec.count
    injected = sum(demand.values())

    with capsys.disabled():
        print(
            f"\nworld_prose @ prose_llm scale: patients={len(world.patients)} "
            f"pools={pools} demand={demand} injected={injected} "
            f"eligibility={eligibility}"
        )

    # the AGGREGATE margin (necessary, not sufficient)
    assert sum(pools.values()) > injected, (
        f"disjoint-eligible maximum {sum(pools.values())} does not exceed the "
        f"preset's {injected} injected corruptions (pools {pools})"
    )
    # the BINDING per-pool margins: this is what the planner fails closed on
    for pool, wanted in sorted(demand.items()):
        assert wanted <= pools[pool], (
            f"pool {pool!r} is overrun: {wanted} corruptions requested of "
            f"{pools[pool]} eligible facts — the planner fails closed here"
        )


# -- world_smoke untouched -------------------------------------------------


def _combined_digest(paths: list[Path]) -> str:
    lines = [
        f"{p.name}:{hashlib.sha256(p.read_bytes()).hexdigest()}"
        for p in sorted(paths, key=lambda x: x.name)
    ]
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


def test_world_smoke_data_byte_identical():
    """Every world_smoke bundle + its provenance are byte-identical to the
    pinned digest, and the smoke preset is unchanged. Adding world_prose must
    not perturb the existing control world."""
    files = sorted(WORLD_SMOKE_DIR.glob("*.json"))
    assert _combined_digest(files) == WORLD_SMOKE_COMBINED_SHA256, (
        "world_smoke data changed; this task must leave the control world untouched"
    )
    preset = SYNTH_PKG / "presets/smoke.yaml"
    assert hashlib.sha256(preset.read_bytes()).hexdigest() == SMOKE_PRESET_SHA256, (
        "smoke.yaml preset changed; this task must leave it untouched"
    )


def test_world_smoke_still_loads_eight_patients():
    prov = json.loads((WORLD_SMOKE_DIR / "provenance.json").read_text())
    assert prov["population"] == 8 and prov["seed"] == 749202607
    world = world_from_synthea_dir(
        WORLD_SMOKE_DIR, reference_date=prov["reference_date"], provenance=prov
    )
    assert len(world.patients) == 8
    # the smoke preset still resolves and points at the smoke world
    assert load_preset("smoke").world_path == "world_smoke"
