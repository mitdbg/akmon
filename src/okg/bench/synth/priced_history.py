"""The priced corpus's declared history sizing, and the check that it held.

PACT ``concord-row-publication-ready``, ``req.row.priced``. The controlled
fixture's dollar cells need a corpus that is BOTH priced and still corrupted at
the end of a history long enough to exercise reuse. ``presets/priced_history``
supplies the prices; this module supplies the other half of the requirement —
the SIZING, declared up front and checked against what the generated history
actually did:

* how many corruption sites the corpus seals,
* how many of them the history PLANTS,
* how many it UNPLANTS again, and
* how many are therefore expected LIVE at the final update.

WHY THE DECLARATION LIVES HERE AND NOT IN THE PRESET YAML. ``SynthConfig`` is a
strict model whose full dump is the ``config_digest``, so a new field on it moves
every preset's digest and regenerates every golden scorecard pinned to one —
which is precisely the ripple ``req.row.priced`` rejects for the prices
themselves. The sizing is also not a property of the CONFIG: it is a property of
the (config, history-config) pair, and ``HistoryConfig`` is deliberately outside
the config digest for the same reason. So the declaration is a sealed constant
keyed by preset name, and :func:`assert_declared_sizing` is the run's check.

WHY A CHECK AT ALL. A history that quietly stops corrupting produces an EMPTY
live roster at the final step, and an empty roster is the failure mode this whole
requirement exists to rule out: every detection cell computed over it is vacuous
and the dollar comparison is a comparison of nothing. Left unchecked, a corpus
that drifted into that state would still produce a row that LOOKS complete. There
is no default and no fallback: a preset with no declaration is an error, and a
realized count that misses the declaration is an error, never a warning.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from okg.bench.synth.history import DELTA_PLANT_ERROR, DELTA_UNPLANT_ERROR

#: The preset whose corpus is priced AND still corrupted at the final update.
PRICED_PRESET = "priced_history"


class PricedHistoryError(ValueError):
    """A declared/realized sizing mismatch, or an undeclared preset."""


@dataclass(frozen=True)
class HistorySizing:
    """The corruption arithmetic of one (preset, history) pair.

    ``live_at_final`` is not independent: a corruption is live at the end iff it
    was planted and not unplanted, so it must equal ``planted - unplanted``. The
    identity is checked on construction, which keeps a DECLARATION from being
    internally inconsistent as well as keeping a realized reading honest.
    """

    corruption_sites: int
    planted: int
    unplanted: int
    live_at_final: int

    def __post_init__(self) -> None:
        for name in ("corruption_sites", "planted", "unplanted", "live_at_final"):
            value = getattr(self, name)
            if not isinstance(value, int) or value < 0:
                raise PricedHistoryError(f"sizing field {name}={value!r} is not a count")
        if self.planted > self.corruption_sites:
            raise PricedHistoryError(
                f"sizing plants {self.planted} corruption(s) from a corpus that "
                f"seals only {self.corruption_sites}"
            )
        if self.unplanted > self.planted:
            raise PricedHistoryError(
                f"sizing unplants {self.unplanted} corruption(s) but plants only "
                f"{self.planted}"
            )
        if self.live_at_final != self.planted - self.unplanted:
            raise PricedHistoryError(
                f"sizing says {self.live_at_final} corruption(s) live at the final "
                f"update, but plants {self.planted} and unplants {self.unplanted}, "
                f"which leaves {self.planted - self.unplanted}"
            )

    def as_json(self) -> dict[str, int]:
        return {
            "corruption_sites": self.corruption_sites,
            "planted": self.planted,
            "unplanted": self.unplanted,
            "live_at_final": self.live_at_final,
        }


#: What each priced preset DECLARES its 22-update history will do. Read off a
#: generated history once and sealed here, so a later drift in the corruption
#: planner, the delta draw weights or ``HistoryConfig`` moves the realized
#: numbers away from these and fails.
#:
#: ``priced_history`` is smoke's corpus with prices added (same seed, same axes),
#: so these are also the numbers smoke's history realizes: 11 sealed corruption
#: sites, of which the 22 updates plant 9 and unplant 2, leaving 7 live at the
#: final update.
DECLARED_SIZING: Mapping[str, HistorySizing] = {
    PRICED_PRESET: HistorySizing(
        corruption_sites=11, planted=9, unplanted=2, live_at_final=7
    ),
}


def realized_sizing(manifest: Mapping[str, Any], history: Any) -> HistorySizing:
    """Read the sizing a generated history ACTUALLY realized.

    Every number comes from the sealed artifacts — the manifest's gold roster and
    the history's own deltas and per-step live rosters — so this is a reading, not
    a re-derivation: nothing here recomputes what the generator decided.
    """
    corruptions = manifest["gold"]["corruptions"]
    sites = len({site["corruption_id"] for site in corruptions})
    steps = list(history.steps)
    if not steps:
        raise PricedHistoryError(
            "the history has no steps, so its final live-corruption roster is "
            "undefined; a row built on it would grade nothing"
        )
    planted = sum(1 for step in steps if step.delta.kind == DELTA_PLANT_ERROR)
    unplanted = sum(1 for step in steps if step.delta.kind == DELTA_UNPLANT_ERROR)
    return HistorySizing(
        corruption_sites=sites,
        planted=planted,
        unplanted=unplanted,
        live_at_final=len(steps[-1].live_corruptions),
    )


def assert_declared_sizing(
    *, preset_name: str, manifest: Mapping[str, Any], history: Any
) -> HistorySizing:
    """FAIL-CLOSED: the realized history must match what the preset declared.

    Returns the realized sizing so a caller can print it beside the row. Raises
    :class:`PricedHistoryError` when the preset declares nothing, when the
    realized counts differ from the declaration, or when nothing is live at the
    final update — the last stated separately because an empty roster is the
    specific failure the declaration exists to catch, and a caller reading the
    message should not have to infer it from a count mismatch.
    """
    declared = DECLARED_SIZING.get(preset_name)
    if declared is None:
        raise PricedHistoryError(
            f"preset {preset_name!r} declares no history sizing, so a run over it "
            f"has nothing to check its live-corruption roster against; declare one "
            f"in DECLARED_SIZING (declared: {sorted(DECLARED_SIZING)})"
        )
    realized = realized_sizing(manifest, history)
    if realized.live_at_final == 0:
        raise PricedHistoryError(
            f"preset {preset_name!r}: NO corruption is live at the final update of "
            f"the history ({realized.planted} planted, {realized.unplanted} "
            f"unplanted). Every detection cell graded at that step would be "
            f"vacuous and the dollar comparison would grade an uncorrupted corpus"
        )
    if realized != declared:
        raise PricedHistoryError(
            f"preset {preset_name!r}: the history realized {realized.as_json()} but "
            f"the preset declares {declared.as_json()}. The sizing is a claim about "
            f"the corpus the row is measured on, so a drift is a failure, not a "
            f"re-baseline: either the corpus changed or the declaration is wrong"
        )
    return realized
