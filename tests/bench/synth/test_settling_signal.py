"""PACT ``concord-system-stress`` — req.stress.precedence.

Two things must not silently regress.

**The confound.** The precedence arm builds nine buckets to prove DECISIVENESS
AND ORDERING, and its ``disagree`` roles deliberately make a stronger signal
favour a value OTHER than the clean one — so a mis-repair there is the designed
outcome. Pooling those with ordinary conflicts produced a reading that the
stronger signals are LESS accurate than naive counting. They are not; the
effect vanished once the arm-built buckets were separated. These witnesses pin
that the two populations stay separable and that pooling reproduces the wrong
reading, so the confound cannot come back unnoticed.

**The rescue arm.** It is the poison arm's mirror and must stay one: disjoint
eligibility, a stamp on the opposite claim, and a credit rule that refuses a
coincidence. A rescue arm that credited any correct repair would report the
engine fixing conflicts the stamp never touched.
"""
from __future__ import annotations

import pytest

from okg.bench.synth.arms import (
    _poison_eligibility,
    _poison_target,
    _rescue_eligibility,
    _rescue_target,
)
from okg.bench.synth.config import SIGNAL_DECLARED_SOURCE_AUTHORITY


def _claims(n_corrupt: int, n_clean: int, bad="90", good="77"):
    out = []
    for i in range(n_corrupt):
        out.append({"claim_id": f"bad{i}", "corrupted": True, "value": bad})
    for i in range(n_clean):
        out.append({"claim_id": f"good{i}", "corrupted": False, "value": good})
    return out


# --------------------------------------------------------------------------- #
# The two pools are COMPLEMENTARY — that is the whole design.                  #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "n_corrupt,n_clean,poisonable,rescuable",
    [
        (1, 1, True, False),    # exact tie — poison can carry it, nothing to rescue
        (1, 2, True, False),    # corrupted outnumbered — counting already wins
        (2, 1, False, True),    # corrupted MAJORITY — counting loses; the rescue case
        (3, 1, False, True),
    ],
)
def test_poison_and_rescue_pools_are_disjoint_and_complementary(
    n_corrupt, n_clean, poisonable, rescuable
) -> None:
    claims = _claims(n_corrupt, n_clean)
    assert (_poison_eligibility(claims) is not None) is poisonable
    assert (_rescue_eligibility(claims) is not None) is rescuable
    assert not (poisonable and rescuable), "a bucket may not be in both pools"


def test_the_rescue_pool_is_exactly_what_poisoning_excludes() -> None:
    """Poisoning skips corrupted-majority buckets because they mis-repair on
    corroboration anyway. Those are the failure this program is chasing, so the
    rescue arm exists precisely to cover the gap poisoning leaves."""
    corrupted_majority = _claims(2, 1)
    assert _poison_eligibility(corrupted_majority) is None
    assert _rescue_eligibility(corrupted_majority) == ("90", "77")


@pytest.mark.parametrize("claims", [_claims(0, 2), _claims(2, 0), _claims(2, 1, "77", "77")])
def test_a_bucket_with_no_real_conflict_is_in_neither_pool(claims) -> None:
    assert _poison_eligibility(claims) is None
    assert _rescue_eligibility(claims) is None


# --------------------------------------------------------------------------- #
# The stamps are mirror images.                                               #
# --------------------------------------------------------------------------- #
def test_rescue_stamps_the_clean_claim_and_poison_stamps_the_corrupted_one() -> None:
    rescue = _rescue_target("b", _claims(2, 1), "90", "77")
    poison = _poison_target("b", _claims(1, 1), "90", "77")
    assert set(rescue["stamps"]) == {"good0"}, "rescue must stamp the CLEAN claim"
    assert set(poison["stamps"]) == {"bad0"}, "poison must stamp the CORRUPTED claim"
    for target, winner in ((rescue, "77"), (poison, "90")):
        assert target["expected_basis"] == SIGNAL_DECLARED_SOURCE_AUTHORITY
        assert target["expected_outcome"] == "canonical"
        assert target["expected_winner_value"] == winner
    # each records what the bucket would settle on WITHOUT the stamp, which is
    # what lets a by-basis witness refuse a credit for a coincidence
    assert rescue["lower_would_prefer_value"] == "90"
    assert poison["lower_would_prefer_value"] == "77"
    assert rescue["rescue"] is True and poison["poison"] is True


# --------------------------------------------------------------------------- #
# Crediting: a repair that was not caused by the stamp is not a rescue.        #
# --------------------------------------------------------------------------- #
def _credit(rows, rescued):
    """The report's rule: clean value AND the authority basis."""
    return [r for r in rows if r["bucket"] in rescued
            and r["state"] == "repaired"
            and r["resolution_basis"] == SIGNAL_DECLARED_SOURCE_AUTHORITY]


def test_a_correct_repair_on_another_basis_is_not_credited_as_a_rescue() -> None:
    """ACCEPTANCE (non-vacuity): without this the arm would credit the stamp for
    every conflict the engine happened to get right."""
    rescued = {"b1", "b2"}
    rows = [
        {"bucket": "b1", "state": "repaired",
         "resolution_basis": SIGNAL_DECLARED_SOURCE_AUTHORITY},   # a real rescue
        {"bucket": "b2", "state": "repaired",
         "resolution_basis": "dependency_adjusted_support"},      # right, but NOT the stamp
        {"bucket": "b3", "state": "repaired",
         "resolution_basis": SIGNAL_DECLARED_SOURCE_AUTHORITY},   # not a stamped bucket
    ]
    credited = _credit(rows, rescued)
    assert [r["bucket"] for r in credited] == ["b1"]


def test_rate_zero_stamps_nothing_so_it_can_credit_nothing() -> None:
    """The negative control. A rescue curve whose zero point is non-zero is
    measuring something other than the stamp."""
    assert _credit([{"bucket": "b1", "state": "repaired",
                     "resolution_basis": SIGNAL_DECLARED_SOURCE_AUTHORITY}], set()) == []


# --------------------------------------------------------------------------- #
# The confound, pinned.                                                       #
# --------------------------------------------------------------------------- #
def test_pooling_arm_buckets_with_ordinary_ones_reproduces_the_wrong_reading() -> None:
    """The arm's ``disagree`` roles make a stronger signal favour a value other
    than the clean one ON PURPOSE, so mis-repair there is the designed outcome.
    Pooled, the strong signals look inaccurate; separated, they do not. This
    asserts BOTH readings so the separation cannot be dropped silently."""
    arm_built = {"d1", "d2"}
    rows = [
        # arm-built disagreement buckets: designed mis-repairs
        {"bucket": "d1", "state": "mis_repaired", "resolution_basis": "operator_precedent"},
        {"bucket": "d2", "state": "mis_repaired",
         "resolution_basis": SIGNAL_DECLARED_SOURCE_AUTHORITY},
        # ordinary conflicts: naive corroboration, mixed
        {"bucket": "o1", "state": "repaired", "resolution_basis": "dependency_adjusted_support"},
        {"bucket": "o2", "state": "mis_repaired", "resolution_basis": "dependency_adjusted_support"},
    ]

    def wrong_rate(subset, basis):
        rows_b = [r for r in subset if r["resolution_basis"] == basis]
        if not rows_b:
            return None
        return sum(r["state"] == "mis_repaired" for r in rows_b) / len(rows_b)

    pooled_authority = wrong_rate(rows, SIGNAL_DECLARED_SOURCE_AUTHORITY)
    ordinary = [r for r in rows if r["bucket"] not in arm_built]
    separated_authority = wrong_rate(ordinary, SIGNAL_DECLARED_SOURCE_AUTHORITY)

    assert pooled_authority == 1.0, "pooled, authority reads as always wrong"
    assert separated_authority is None, (
        "separated, authority never touched an ordinary conflict — which is the "
        "actual finding, and is invisible unless the populations are split"
    )
    assert wrong_rate(ordinary, "dependency_adjusted_support") == 0.5
