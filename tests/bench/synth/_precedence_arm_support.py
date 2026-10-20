"""Shared helpers for the precedence-arm witnesses (synth-precedence-arms).

Not a test module — the ``test_precedence_*`` files import from it. Keeps the
arm CONFIG and the pure ClaimSignals reconstruction in one place so the declare /
emit / ordering witnesses all speak of the same sealed plan.
"""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from okg.bench.synth.arms import _bucket_claims
from okg.bench.synth.config import ADMITTABLE_SIGNALS, SynthConfig, load_config
from okg.bench.synth.corruption import Planner
from okg.bench.synth.fhir_map import world_from_synthea_dir
from okg.bench.synth.goldseal import build_manifest
from okg.bench.synth.prf import SynthPrf
from okg.bench.synth.render import render_corpus
from okg.bench.synth.runner import _subset_world

_REPO_ROOT = Path(__file__).resolve().parents[3]
WORLD_DIR = _REPO_ROOT / "src/okg/bench/synth/world_smoke"
DEP_DIR = _REPO_ROOT / "deployments" / "synthetic-corpus-smoke"

# The arm seed/config: redundancy 2..3 + 20 value_perturbations yields >= 7
# one-vs-one and >= 2 majority conflicted buckets on this world — enough for the
# 4 isolation + 4 disagreement + 1 corroboration roles the all-signals arm needs.
ARM_SEED = "arms-c1"


def arm_config_dict(admits: Sequence[str]) -> dict[str, Any]:
    return {
        "seed": ARM_SEED,
        "preset_name": "custom",
        "world_path": "world_smoke",
        "benign_variation": {"unit_rescale_rate": 0.0},
        "scale": {"patients": 8, "max_facts_per_patient": 40},
        "redundancy": {"renderings_per_fact_min": 2, "renderings_per_fact_max": 3},
        "corruption": {"classes": {
            "value_perturbation": {
                "count": 20, "magnitude": 0.3, "visibility_corrupted": 1,
            },
        }},
        "precedence_arm": {"admits": list(admits)},
    }


def all_signals_config() -> SynthConfig:
    return load_config(arm_config_dict(sorted(ADMITTABLE_SIGNALS)))


def default_arm_config() -> SynthConfig:
    return load_config(arm_config_dict([]))


def build_arm_manifest(config: SynthConfig) -> dict[str, Any]:
    """The sealed manifest for an arm config, offline (no DB)."""
    prov = json.loads((WORLD_DIR / "provenance.json").read_text())
    world = world_from_synthea_dir(
        WORLD_DIR, reference_date=prov["reference_date"], provenance=prov
    )
    world = _subset_world(world, config.scale.patients)
    prf = SynthPrf(config.seed)
    planner = Planner(world, config, prf)
    plan = planner.plan()
    rendered = render_corpus(world, config, planner, plan, prf)
    return build_manifest(config=config, world=world, plan=plan, rendered=rendered)


def signals_for_target(
    target: Mapping[str, Any], bucket_claims: Mapping[str, Any]
):
    """Reconstruct the bucket's ClaimSignals + precedents from the sealed target.

    Mirrors the deployment signal surface: each claim's ranking attrs come from
    the sealed stamps (default otherwise) and independent_support is the ENGINE's
    computed value (distinct sources = distinct docs asserting the value). Returns
    (signals, precedents, value_by_claim_id).
    """
    from okg.substrate.consistency.claim_model.precedence import (
        ClaimSignals,
        Precedent,
    )

    claims = bucket_claims[target["bucket"]]
    support = Counter(c["value"] for c in claims)
    signals = []
    for c in claims:
        stamp = target["stamps"].get(c["claim_id"], {})
        signals.append(ClaimSignals(
            claim_id=c["claim_id"],
            source_class=stamp.get("source_class", "wiki"),
            scope="",
            is_superseded=(stamp.get("is_superseded") == "true"),
            effective_date=stamp.get("effective_date", ""),
            source_authority=int(stamp.get("source_authority", 0)),
            independent_support=support[c["value"]],
            value=c["value"],
        ))
    precedents = [
        Precedent(
            prefer_source=p["prefer_source"],
            scope=p["scope"],
            sequence=int(p["sequence"]),
        )
        for p in target["precedents"]
    ]
    return signals, precedents, {c["claim_id"]: c["value"] for c in claims}


def bucket_claims_of(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return _bucket_claims(manifest["trace"], manifest["patients"])
