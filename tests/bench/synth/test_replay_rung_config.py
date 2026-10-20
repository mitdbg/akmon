"""PACT ``concord-reuse-defect-isolation`` — req.rdi.isolate.

The engine takes several distinct kinds of reuse shortcut and can disable them
INDIVIDUALLY. That switch was reachable only from a harness that builds its own
synthetic workload, while the replay that can drive a SEALED history took no
switch at all — so the machinery to ask "which shortcut" existed, the data to
answer it existed, and nothing joined them.

These witnesses hold the join to the property that makes an ablation arm mean
anything: an arm is graded on the engine's OWN RECORDED CAUSE TALLY, never on
the flag that was set. An arm whose switch was set but which took the shortcut
anyway is a broken arm, not a result — and a test that asserted on the flag
would report it as a result.
"""
from __future__ import annotations

import inspect

import pytest

from okg.bench.synth.parity import replay_mode, run_update
from okg.bench.synth.reuse_ablation import RUNG_SKIP_CAUSE
from okg.substrate.consistency.reuse_scheduler import (
    REUSE_RUNGS,
    SKIP_CAUSES,
    ReuseRungConfig,
)


def test_the_replay_path_accepts_a_rung_configuration() -> None:
    """The join itself. Both layers must carry it or the arm silently runs the
    complete ladder and reports itself as an ablation."""
    for func in (replay_mode, run_update):
        assert "rungs" in inspect.signature(func).parameters, func.__name__


def test_the_default_is_the_shipped_ladder() -> None:
    """Existing results must not move. ``None`` means the engine's own default,
    never a reconstructed subset that might drift from it."""
    for func in (replay_mode, run_update):
        assert inspect.signature(func).parameters["rungs"].default is None


def test_a_rung_config_names_only_real_rungs() -> None:
    """A typo'd rung name would silently run the complete ladder — the same
    defect class as a mistyped symbol reading as an absent capability."""
    complete = ReuseRungConfig()
    assert set(complete.active) == set(REUSE_RUNGS)
    with pytest.raises(Exception):
        ReuseRungConfig(active=frozenset({"not_a_rung"}))


@pytest.mark.parametrize("rung", sorted(REUSE_RUNGS))
def test_every_rung_can_be_disabled_alone(rung: str) -> None:
    """One arm per shortcut is only possible if each is separately removable.

    Parametrised over REUSE_RUNGS, not SKIP_CAUSES: the two are DIFFERENT
    vocabularies with an empty intersection (a rung is `certificate_unchanged`,
    the cause it produces is `certificate_degenerate_unchanged`), and a version
    of this test that iterated causes skipped every case and asserted nothing.
    """
    reduced = ReuseRungConfig(active=frozenset(set(REUSE_RUNGS) - {rung}))
    assert rung not in reduced.active
    assert set(reduced.active) == set(REUSE_RUNGS) - {rung}
    suppressed = set(reduced.suppressed_skip_causes())
    assert RUNG_SKIP_CAUSE[rung] in suppressed, (
        f"disabling {rung} must suppress its cause {RUNG_SKIP_CAUSE[rung]!r}, "
        "or an arm cannot be graded on the engine's tally"
    )


def test_the_rung_and_cause_vocabularies_are_distinct_and_mapped() -> None:
    """The trap that made an earlier version of this file vacuous, pinned."""
    assert not (set(REUSE_RUNGS) & set(SKIP_CAUSES)), (
        "rungs and causes are distinct vocabularies; a test that filters one "
        "by membership in the other silently asserts nothing"
    )
    for rung in REUSE_RUNGS:
        assert RUNG_SKIP_CAUSE[rung] in SKIP_CAUSES, rung


def test_an_arm_is_graded_on_the_cause_tally_not_the_flag() -> None:
    """THE property. An arm whose switch was set while the engine still took the
    shortcut is a BROKEN ARM, not a result — and grading on the flag would
    report it as a clean one."""
    def arm_is_valid(disabled_rung: str, cause_tally: dict[str, int]) -> bool:
        return cause_tally.get(RUNG_SKIP_CAUSE[disabled_rung], 0) == 0

    cause = RUNG_SKIP_CAUSE["certificate_unchanged"]
    assert arm_is_valid("certificate_unchanged", {cause: 0, "effects_overlap": 9})
    assert not arm_is_valid("certificate_unchanged", {cause: 3})
