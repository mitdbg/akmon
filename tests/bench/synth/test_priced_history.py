"""PACT ``concord-row-publication-ready``, ``req.row.priced``.

The priced corpus's two halves, checked without a database.

* The preset SEALS a non-zero, non-uniform action price table — the table the
  cost presets already seal — while leaving smoke's corpus byte-identical, so a
  dollar figure measured on it differs from smoke's only in the economics.
* The history SIZING the preset declares is the sizing the generated history
  realizes, and the check that says so fails loudly on an empty final roster, on
  a drifted count, and on a preset that declares nothing at all. An empty
  live-corruption roster at the final update is the failure this requirement
  exists to rule out: every detection cell over it is vacuous and the dollar
  comparison grades an uncorrupted corpus.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

import pytest

from okg.bench.synth.config import ACTION_CLASSES, load_preset
from okg.bench.synth.history import DELTA_PLANT_ERROR, DELTA_UNPLANT_ERROR
from okg.bench.synth.priced_history import (
    DECLARED_SIZING,
    PRICED_PRESET,
    HistorySizing,
    PricedHistoryError,
    assert_declared_sizing,
    realized_sizing,
)


# =========================================================================== #
# The preset seals real prices, and changes nothing else                       #
# =========================================================================== #
def test_the_priced_preset_seals_non_zero_non_uniform_action_prices() -> None:
    prices = load_preset(PRICED_PRESET).cost.action_prices.as_table()
    assert set(prices) == set(ACTION_CLASSES)
    assert all(value > 0.0 for value in prices.values()), prices
    assert len(set(prices.values())) == len(prices), (
        f"a uniform price table cannot separate modes that differ in WHICH "
        f"actions they perform: {prices}"
    )


def test_the_prices_are_the_table_the_cost_presets_already_seal() -> None:
    """Not invented here: the amounts come from the sealed cost presets."""
    priced = load_preset(PRICED_PRESET).cost.action_prices.as_table()
    for source in ("cost_weighted", "cost_mandatory"):
        assert priced == load_preset(source).cost.action_prices.as_table(), source


def test_the_priced_preset_leaves_the_smoke_corpus_alone() -> None:
    """Same seed and same axes: only the economics move, so the dollar row is
    measured on the corpus the unpriced row is measured on."""
    smoke = load_preset("smoke").model_dump(mode="json")
    priced = load_preset(PRICED_PRESET).model_dump(mode="json")
    moved = sorted(key for key in smoke if smoke[key] != priced[key])
    assert moved == ["cost", "preset_name"], moved
    assert load_preset("smoke").cost.action_prices.as_table() == {
        name: 0.0 for name in ACTION_CLASSES
    }, "the smoke preset must stay UNPRICED — it is the honest-zero witness"


# =========================================================================== #
# The sizing declaration, and every way it refuses                             #
# =========================================================================== #
@dataclass(frozen=True)
class _Delta:
    kind: str


@dataclass(frozen=True)
class _Step:
    delta: _Delta
    live_corruptions: tuple[str, ...] = ()


@dataclass(frozen=True)
class _History:
    steps: tuple[_Step, ...]


def _stub(planted: int, unplanted: int, live: int, sites: int = 11) -> tuple[Any, Any]:
    """A history with the requested delta counts and final live roster."""
    steps = [_Step(_Delta(DELTA_PLANT_ERROR)) for _ in range(planted)]
    steps += [_Step(_Delta(DELTA_UNPLANT_ERROR)) for _ in range(unplanted)]
    steps.append(_Step(_Delta("reaffirmation"), tuple(f"corr:{i}" for i in range(live))))
    manifest = {
        "gold": {"corruptions": [{"corruption_id": f"corr:{i}"} for i in range(sites)]}
    }
    return manifest, _History(tuple(steps))


def test_the_declared_sizing_is_the_sizing_the_stub_realizes() -> None:
    declared = DECLARED_SIZING[PRICED_PRESET]
    manifest, history = _stub(
        declared.planted, declared.unplanted, declared.live_at_final,
        declared.corruption_sites,
    )
    assert realized_sizing(manifest, history) == declared
    assert assert_declared_sizing(
        preset_name=PRICED_PRESET, manifest=manifest, history=history
    ) == declared


def test_an_empty_final_roster_fails_loudly_rather_than_grading_nothing() -> None:
    """The acceptance names this one: an empty roster fails the run."""
    declared = DECLARED_SIZING[PRICED_PRESET]
    manifest, history = _stub(declared.planted, declared.planted, 0)
    with pytest.raises(PricedHistoryError, match="NO corruption is live"):
        assert_declared_sizing(
            preset_name=PRICED_PRESET, manifest=manifest, history=history
        )


def test_a_history_that_stops_corrupting_short_of_the_declaration_is_refused() -> None:
    declared = DECLARED_SIZING[PRICED_PRESET]
    manifest, history = _stub(
        declared.planted - 1, declared.unplanted, declared.live_at_final - 1,
        declared.corruption_sites,
    )
    with pytest.raises(PricedHistoryError, match="the preset declares"):
        assert_declared_sizing(
            preset_name=PRICED_PRESET, manifest=manifest, history=history
        )


def test_a_preset_with_no_declaration_has_nothing_to_check_against() -> None:
    manifest, history = _stub(9, 2, 7)
    with pytest.raises(PricedHistoryError, match="declares no history sizing"):
        assert_declared_sizing(
            preset_name="smoke", manifest=manifest, history=history
        )


def test_an_internally_inconsistent_declaration_is_refused_on_construction() -> None:
    """planted - unplanted is not a free parameter."""
    with pytest.raises(PricedHistoryError, match="which leaves"):
        HistorySizing(corruption_sites=11, planted=9, unplanted=2, live_at_final=6)
    with pytest.raises(PricedHistoryError, match="seals only"):
        HistorySizing(corruption_sites=2, planted=9, unplanted=2, live_at_final=7)
    with pytest.raises(PricedHistoryError, match="but plants only"):
        HistorySizing(corruption_sites=11, planted=2, unplanted=9, live_at_final=0)
    with pytest.raises(PricedHistoryError, match="is not a count"):
        HistorySizing(corruption_sites=11, planted=9, unplanted=2, live_at_final=-7)


def test_a_history_with_no_steps_has_no_final_roster() -> None:
    manifest, _ = _stub(0, 0, 0)
    with pytest.raises(PricedHistoryError, match="no steps"):
        realized_sizing(manifest, _History(()))


def test_every_declaration_in_the_table_names_a_real_preset() -> None:
    for name in DECLARED_SIZING:
        assert load_preset(name).preset_name == name


# =========================================================================== #
# The REAL corpus realizes the declaration                                     #
# =========================================================================== #
def test_the_generated_priced_history_realizes_its_declared_sizing(
    world, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The corpus and its 22-update history, generated for real (no database).

    This is the check that makes the declaration a claim about the corpus rather
    than about a stub: if the corruption planner, the delta draw weights or the
    history config drift, the realized numbers move off the declaration here,
    before any database is touched.
    """
    from okg.bench.synth.corruption import Planner
    from okg.bench.synth.goldseal import build_manifest
    from okg.bench.synth.history import HistoryConfig, build_history
    from okg.bench.synth.prf import SynthPrf
    from okg.bench.synth.render import render_corpus

    config = load_preset(PRICED_PRESET)
    prf = SynthPrf(config.seed)
    planner = Planner(world, config, prf)
    plan = planner.plan()
    rendered = render_corpus(world, config, planner, plan, prf)
    manifest = build_manifest(
        config=config, world=world, plan=plan, rendered=rendered
    )
    history = build_history(
        config=config, world=world, manifest=manifest,
        base_docs=rendered.clean_docs, history_config=HistoryConfig(updates=22),
    )

    realized = assert_declared_sizing(
        preset_name=PRICED_PRESET, manifest=manifest, history=history
    )
    assert realized == DECLARED_SIZING[PRICED_PRESET], realized.as_json()
    assert realized.live_at_final > 0
    assert len(history.steps[-1].live_corruptions) == realized.live_at_final

    # NON-VACUITY on the real corpus: move the declaration by one and the same
    # call refuses. Without this the check could be passing because it compares
    # a number to itself. Patched by dotted path rather than by importing the
    # module a second way, and restored by the fixture rather than by hand.
    bumped = replace(
        DECLARED_SIZING[PRICED_PRESET],
        unplanted=DECLARED_SIZING[PRICED_PRESET].unplanted + 1,
        live_at_final=DECLARED_SIZING[PRICED_PRESET].live_at_final - 1,
    )
    monkeypatch.setattr(
        "okg.bench.synth.priced_history.DECLARED_SIZING",
        {PRICED_PRESET: bumped},
    )
    with pytest.raises(PricedHistoryError, match="the preset declares"):
        assert_declared_sizing(
            preset_name=PRICED_PRESET, manifest=manifest, history=history
        )
