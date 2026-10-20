"""PACT ``concord-measurement-integrity`` — req.mi.seal-regression.

Every stored benchmark result is measured against a corpus whose identity is a
set of digests: the config, the sealed manifest, the history, and the chain that
covers document BYTES. Those digests are quoted in sealed rows and pinned
goldens — and until this module, no test re-derived them. A change to the
generator, the config schema, or anything feeding them could invalidate every
stored result while every test still passed.

That is not hypothetical. ``concord-system-stress`` added an off-by-default axis
to the digested config and emitted its block unconditionally into the sealed arm
plan, and moved ALL FOUR digests of the capstone corpus. It was caught only
because an unrelated fix happened to require re-deriving them by hand.

So: unsealing the corpus is now a TEST FAILURE, not a discovery made later by
whoever happens to look.

Responding to a failure here
----------------------------
* If the corpus genuinely changed — a new error class, a different seed, a
  changed renderer — re-pin these constants DELIBERATELY, in the same commit,
  and expect every sealed row quoting the old digest to be re-derived too.
* If it changed by accident — typically a new field entering a digested schema
  — make the new axis digest-neutral AT ITS DEFAULT rather than re-pinning. An
  axis that is off does not change the corpus and must not change its identity.
"""
from __future__ import annotations

import json
import pathlib

import pytest

from okg.bench.synth.config import load_config, load_preset
from okg.bench.synth.corruption import Planner
from okg.bench.synth.fhir_map import world_from_synthea_dir
from okg.bench.synth.goldseal import build_manifest
from okg.bench.synth.history import HistoryConfig, build_history, verify_chain
from okg.bench.synth.prf import SynthPrf
from okg.bench.synth.render import render_corpus

#: The capstone corpus's sealed identity, as quoted by
#: ``pact/changes/concord-bench-e2e/reports/controlled-fixture-row.md``.
SEALED_SMOKE = {
    "config": "aa415cee069ec9c4d3c983364ba0983561d5b5da34d25ee51cfda977b9a634c6",
    "manifest": "8061128484ff195dac385a81531f7bdde46b0e3f8e440d9aecd1d179c177115a",
    "history": "6acc07903b1ff29acff36a982f14c4acff6c50444357cee48e9c9649d7ff12d7",
    "chain": "f3156494ff5e29adbbfa5d187235e1d6bf69b198f5946def6fda128cd5f4e59f",
}
SEALED_UPDATES = 22
_WORLD = pathlib.Path(__file__).resolve().parents[3] / "src/okg/bench/synth/world_smoke"


def _rederive(config) -> dict[str, str]:
    provenance = json.loads((_WORLD / "provenance.json").read_text(encoding="utf-8"))
    world = world_from_synthea_dir(
        _WORLD, reference_date=provenance["reference_date"], provenance=provenance
    )
    prf = SynthPrf(config.seed)
    planner = Planner(world, config, prf)
    plan = planner.plan()
    rendered = render_corpus(world, config, planner, plan, prf)
    manifest = build_manifest(
        config=config, world=world, plan=plan, rendered=rendered
    )
    history = build_history(
        config=config, world=world, manifest=manifest,
        base_docs=rendered.clean_docs,
        history_config=HistoryConfig(updates=SEALED_UPDATES),
    )
    assert not verify_chain(history), "the re-derived gold chain is broken"
    return {
        "config": config.config_digest(),
        "manifest": manifest["manifest_digest"],
        "history": history.history_digest,
        "chain": history.steps[-1].chain_digest,
    }


@pytest.fixture(scope="module")
def rederived() -> dict[str, str]:
    return _rederive(load_preset("smoke"))


def test_the_sealed_corpus_still_has_its_sealed_identity(rederived) -> None:
    """Re-derived from the generator, not read back from an artifact."""
    moved = [
        f"{name}: sealed {SEALED_SMOKE[name][:16]}…, re-derived {rederived[name][:16]}…"
        for name in SEALED_SMOKE
        if rederived[name] != SEALED_SMOKE[name]
    ]
    assert not moved, (
        "the sealed corpus's identity MOVED — every stored row and golden that "
        "quotes one of these digests is now measured against a different corpus:"
        "\n  " + "\n  ".join(moved) +
        "\n\nIf the corpus genuinely changed, re-pin these constants deliberately."
        "\nIf a new field entered a digested schema, make it digest-neutral at "
        "its default instead: an axis that is OFF does not change the corpus."
    )


def test_the_guard_is_not_satisfied_by_a_digest_that_ignores_everything(
    rederived,
) -> None:
    """NON-VACUITY. A guard could be trivially satisfied by a digest insensitive
    to its inputs. Turning an axis ON must still move it — otherwise the digest
    would be lying about what corpus a run used."""
    used = load_config({
        **load_preset("smoke").model_dump(),
        "precedence_arm": {
            "admits": [], "surface": {}, "role_redundancy": 1,
            "poison_rate": 0.0, "rescue_rate": 0.5,
        },
    })
    assert used.config_digest() != rederived["config"], (
        "turning the rescue axis ON did not change the config digest — the "
        "digest is insensitive to a change that DOES alter the corpus"
    )


def test_every_sealed_digest_is_distinct(rederived) -> None:
    """Four digests over four different objects. Two that coincided would mean
    one of them is not covering what its name says."""
    assert len(set(rederived.values())) == len(rederived), rederived
