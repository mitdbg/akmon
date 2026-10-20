"""E3 ablation REPORT — the four ways an ablation looks successful and is not.

PACT ``concord-e3-mechanism-ablations``, task.e3.ablation-harness
(req.e3.per-rung-attribution). The live matrix
(``test_reuse_rung_ablation_live.py``) asserts that each of these checks is
EMPTY on the real run. These are the companion witnesses that each check is not
vacuous: every one is fed a measurement that violates it and must report it.

They are pure functions over recorded measurements, so they run without Postgres.
"""
from __future__ import annotations

import json
from typing import Any

import pytest

from okg.bench.synth.reuse_ablation import (
    ABLATION_PRICE_TABLE,
    ATTRIBUTION_METRICS,
    BASELINE_ARM,
    METRIC_COST_DIRECTION,
    NO_RUNGS_ARM,
    OBJECTIVE_BOUNDS_ROW,
    RESIDUAL_METRICS,
    VERDICT_COST_MORE,
    VERDICT_DID_NOT_PAY,
    VERDICT_INSIDE_NOISE,
    VERDICT_MOVED_NO_COST_DIRECTION,
    VERDICT_PAID,
    VERDICT_REDUNDANT,
    AblationReport,
    ArmMeasurement,
    RungAttribution,
    _classify,
    attribute,
    build_workload,
)
from okg.substrate.consistency.reuse_scheduler import (
    CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED,
    CAUSE_SKIP_DOMINANCE,
    CAUSE_SKIP_JUSTIFIED,
    REUSE_RUNGS,
    RUNG_BOUNDED_DOMINANCE,
    RUNG_CERTIFICATE_UNCHANGED,
    RUNG_EFFECT_BOUND,
    RUNG_SKIP_CAUSE,
    ReuseRungConfig,
)
from okg.substrate.errors import SubstrateError

_ANSWER = "answer-digest-shared-by-every-arm"
_BUCKET = '["preferred_target","pref|k1"]'


def _measure(
    *,
    seed: int = 0,
    rungs: ReuseRungConfig | None = None,
    cause_tally: dict[str, int] | None = None,
    answer_digest: str = _ANSWER,
    loser_roster: dict[str, list[str]] | None = None,
    withheld: tuple[str, ...] = (),
    verdicts: dict[str, int] | None = None,
    actions_reused: int = 7,
    charged_units: int = 51,
    cost_usd: float = 0.483,
    **overrides: Any,
) -> ArmMeasurement:
    rungs = rungs if rungs is not None else ReuseRungConfig.all_enabled()
    if cause_tally is None:
        cause_tally = {
            RUNG_SKIP_CAUSE[r]: 1 for r in REUSE_RUNGS if rungs.enabled(r)
        }
    fields: dict[str, Any] = dict(
        seed=seed, arm=rungs.arm_label, rungs=rungs,
        workload=build_workload(seed),
        actions_reached=7, actions_reused=actions_reused,
        analysis_work_units=7, certificate_work_units=7,
        evidence_lifetime_generations=3.0, invalidations=0,
        charged_units=charged_units, cost_usd=cost_usd,
        cost_priced_complete=True,
        savings_vs_reference_units=10, savings_vs_reference_cost=0.1,
        cause_tally=cause_tally,
        verdicts=verdicts or {"equal": 10, "mismatch": 0, "excluded": 0},
        answer_digest=answer_digest,
        loser_roster=loser_roster or {_BUCKET: ["claim:e", "claim:w"]},
        withheld_claim_ids=withheld,
        effective_claim_count=4,
        recorded_differences={},
        pass_digests=("d0",),
    )
    fields.update(overrides)
    return ArmMeasurement(**fields)


def _baseline(seed: int = 0, **kw: Any) -> ArmMeasurement:
    return _measure(seed=seed, rungs=ReuseRungConfig.all_enabled(), **kw)


# =========================================================================== #
# 1) A switch that is SET but changes nothing.                                #
# =========================================================================== #
def test_switch_violation_is_reported_and_refused() -> None:
    """An arm whose disabled rung still stamped a skip is REPORTED, and
    :meth:`ArmMeasurement.check` refuses it outright."""
    rungs = ReuseRungConfig.without(RUNG_BOUNDED_DOMINANCE)
    bad = _measure(
        rungs=rungs,
        cause_tally={CAUSE_SKIP_JUSTIFIED: 1, CAUSE_SKIP_DOMINANCE: 1},
    )
    report = AblationReport(measurements=(_baseline(), bad))
    print(f"\n[report/switch] {report.switch_violations()}")
    assert report.switch_violations() == [
        {"seed": 0, "arm": "minus_bounded_dominance",
         "violations": {CAUSE_SKIP_DOMINANCE: 1}}
    ]
    with pytest.raises(SubstrateError) as excinfo:
        bad.check()
    assert "the switch was set and the behaviour did not change" in str(excinfo.value)

    # ...and a correctly-ablated arm reports nothing.
    good = _measure(rungs=rungs, cause_tally={CAUSE_SKIP_JUSTIFIED: 1})
    assert AblationReport(measurements=(_baseline(), good)).switch_violations() == []
    good.check()


# =========================================================================== #
# 2) Collapsing the ladder instead of removing one rung.                      #
# =========================================================================== #
def test_a_collapsed_ladder_is_reported() -> None:
    """Ablating one rung must not silently stop ANOTHER rung from firing."""
    rungs = ReuseRungConfig.without(RUNG_CERTIFICATE_UNCHANGED)
    # the arm kept effect_bound and bounded_dominance, but only one fired.
    collapsed = _measure(rungs=rungs, cause_tally={CAUSE_SKIP_JUSTIFIED: 1})
    report = AblationReport(measurements=(_baseline(), collapsed))
    print(f"\n[report/collapse] {report.reachability_violations()}")
    assert report.reachability_violations() == [
        {"seed": 0, "arm": "minus_certificate_unchanged",
         "rungs_that_stopped_firing": [RUNG_BOUNDED_DOMINANCE]}
    ]

    intact = _measure(
        rungs=rungs,
        cause_tally={CAUSE_SKIP_JUSTIFIED: 1, CAUSE_SKIP_DOMINANCE: 1},
    )
    assert AblationReport(
        measurements=(_baseline(), intact)
    ).reachability_violations() == []


# =========================================================================== #
# 3) An ablation that changed the ANSWER.                                     #
# =========================================================================== #
def test_an_answer_change_is_reported_loudly() -> None:
    """A rung that is load-bearing for CORRECTNESS rather than cost surfaces
    here — it is never smoothed into the cost table."""
    changed = _measure(
        rungs=ReuseRungConfig.without(RUNG_EFFECT_BOUND),
        answer_digest="a-DIFFERENT-answer",
    )
    report = AblationReport(measurements=(_baseline(), changed))
    violations = report.answer_violations()
    print(f"\n[report/answer] {violations}")
    assert len(violations) == 1
    assert violations[0]["arm"] == "minus_effect_bound"
    assert violations[0]["arm_answer_digest"] == "a-DIFFERENT-answer"
    assert violations[0]["baseline_answer_digest"] == _ANSWER

    same = _measure(rungs=ReuseRungConfig.without(RUNG_EFFECT_BOUND))
    assert AblationReport(measurements=(_baseline(), same)).answer_violations() == []


def test_an_unexplained_roster_difference_is_reported() -> None:
    """The answer digest excludes the loser roster because §9.7 licenses a
    dominance skip to leave a loser unextracted. That exclusion is ACCOUNTABLE:
    a roster difference the withheld claims do not explain is reported."""
    baseline = _baseline(
        withheld=("claim:cr1",),
        loser_roster={_BUCKET: ["claim:e", "claim:w"]},
    )
    # licensed: differs by exactly the claim the baseline withheld.
    licensed = _measure(
        rungs=ReuseRungConfig.without(RUNG_BOUNDED_DOMINANCE),
        loser_roster={_BUCKET: ["claim:cr1", "claim:e", "claim:w"]},
    )
    assert AblationReport(
        measurements=(baseline, licensed)
    ).roster_anomalies() == []

    # unexplained: a claim NEITHER arm withheld appeared/vanished.
    anomalous = _measure(
        rungs=ReuseRungConfig.without(RUNG_BOUNDED_DOMINANCE),
        loser_roster={_BUCKET: ["claim:cr1", "claim:e", "claim:w", "claim:ghost"]},
    )
    report = AblationReport(measurements=(baseline, anomalous))
    print(f"\n[report/roster] {report.roster_anomalies()}")
    assert report.roster_anomalies() == [
        {"seed": 0, "arm": "minus_bounded_dominance", "bucket": _BUCKET,
         "unexplained_claims": ["claim:ghost"]}
    ]


def test_a_mismatch_verdict_is_reported() -> None:
    """An arm whose differential run did not equal the complete reference."""
    bad = _measure(
        rungs=ReuseRungConfig.without(RUNG_EFFECT_BOUND),
        verdicts={"equal": 9, "mismatch": 1, "excluded": 0},
    )
    report = AblationReport(measurements=(_baseline(), bad))
    print(f"\n[report/verdict] {report.verdict_violations()}")
    assert len(report.verdict_violations()) == 1
    assert AblationReport(
        measurements=(_baseline(), _measure(
            rungs=ReuseRungConfig.without(RUNG_EFFECT_BOUND)))
    ).verdict_violations() == []


# =========================================================================== #
# 4) A per-rung number that is inside the noise.                              #
# =========================================================================== #
def test_a_sign_flipping_delta_is_never_published_as_an_effect() -> None:
    """This benchmark has already retracted one effect whose SIGN depended on
    which repeat you took. A delta interval spanning zero is reported INSIDE THE
    NOISE, never as a measured saving."""
    m = "actions_reused"
    assert _classify([-3.0, 4.0], m) == VERDICT_INSIDE_NOISE
    assert _classify([2.0, 0.0], m) == VERDICT_INSIDE_NOISE   # touches zero
    assert _classify([0.0, 0.0, 0.0], m) == VERDICT_DID_NOT_PAY
    assert _classify([-2.0, -1.0, -3.0], m) == VERDICT_PAID
    assert _classify([], m) == VERDICT_INSIDE_NOISE


# =========================================================================== #
# 5) A verdict that reads 'paid' while the delta says the opposite.           #
# =========================================================================== #
def test_the_verdict_carries_DIRECTION_not_merely_significance() -> None:
    """``verdict`` used to be direction-blind: any stable nonzero delta read
    ``paid``. On the recorded run that gave ``effect_bound / cost_usd = -0.040``
    and ``bounded_dominance / cost_usd = +0.102`` the SAME label while meaning
    opposite things, so anyone tabulating ``verdict == "paid"`` drew the wrong
    conclusion on three of six cost rows.

    The polarity is per-metric and cannot be read off the sign: ablating a rung
    LOSES reuses but ADDS charged work."""
    # the two rows from the recorded run that used to collide.
    assert _classify([0.051, 0.102, 0.153], "cost_usd") == VERDICT_PAID
    assert _classify([-0.03, -0.06, -0.03], "cost_usd") == VERDICT_COST_MORE
    assert _classify([-3.0, -6.0, -3.0], "actions_reused") == VERDICT_PAID
    assert _classify([3.0, 6.0], "actions_reused") == VERDICT_COST_MORE
    assert _classify([11.0, 22.0, 33.0], "charged_units") == VERDICT_PAID
    assert _classify([-3.0, -6.0], "charged_units") == VERDICT_COST_MORE
    # a metric with no cost polarity never claims one in either direction.
    for metric in ("evidence_lifetime_generations", "invalidations"):
        assert METRIC_COST_DIRECTION[metric] == 0
        assert _classify([-0.5, -0.67], metric) == VERDICT_MOVED_NO_COST_DIRECTION
        assert _classify([0.33, 0.61], metric) == VERDICT_MOVED_NO_COST_DIRECTION
    # and the raw direction is recorded beside the verdict, so a reader never
    # has to trust the label to carry it.
    a = RungAttribution(
        rung=RUNG_EFFECT_BOUND, metric="cost_usd",
        per_seed_deltas={0: -0.03, 1: -0.06}, verdict=VERDICT_COST_MORE,
    )
    stored = a.to_canonical_dict()
    assert stored["direction"] == "ablation_decreases"
    assert stored["metric_cost_direction"] == +1


def test_a_rung_that_FIRED_is_never_labelled_did_not_pay() -> None:
    """The refuted claim, in one assertion.

    A zero marginal has two causes and they are opposite findings: the workload
    never presented the condition (absence), or another rung re-earned every
    skip (redundancy). ``did_not_pay`` is reserved for the first; a rung whose
    skips another rung re-earned reads ``redundant_on_this_workload``."""
    zero = [0.0, 0.0, 0.0]
    assert _classify(zero, "actions_reused", substituted=False) == (
        VERDICT_DID_NOT_PAY
    )
    # the recorded run's certificate rung: 3 / 4 / 5 skips, marginal zero.
    assert _classify(zero, "actions_reused", substituted=True) == (
        VERDICT_REDUNDANT
    )
    # the distinction is decided ONCE per rung from the recorded skip counts, so
    # it holds on every metric rather than being re-litigated column by column.
    for metric in ATTRIBUTION_METRICS:
        assert _classify(zero, metric, substituted=True) == VERDICT_REDUNDANT
        assert _classify(zero, metric, substituted=False) == VERDICT_DID_NOT_PAY


def test_attribution_is_by_difference_against_the_same_seed() -> None:
    """A per-rung delta is measured against the SAME seed's all-rungs arm — never
    against another run that merely happens to be cheaper."""
    measurements = []
    for seed, (base_reused, abl_reused) in enumerate([(7, 6), (9, 7)]):
        measurements.append(_baseline(seed=seed, actions_reused=base_reused))
        measurements.append(_measure(
            seed=seed, rungs=ReuseRungConfig.without(RUNG_BOUNDED_DOMINANCE),
            actions_reused=abl_reused,
            cause_tally={RUNG_SKIP_CAUSE[RUNG_CERTIFICATE_UNCHANGED]: 1,
                         CAUSE_SKIP_JUSTIFIED: 1},
        ))
    attributions = attribute(measurements)
    reused = next(
        a for a in attributions
        if a.rung == RUNG_BOUNDED_DOMINANCE and a.metric == "actions_reused"
    )
    print(f"\n[report/attr] {reused.to_canonical_dict()}")
    assert reused.per_seed_deltas == {0: -1.0, 1: -2.0}
    assert reused.spread == (-2.0, -1.0)
    assert reused.verdict == VERDICT_PAID
    # every attribution carries every seed — never a single draw.
    for a in attributions:
        assert len(a.per_seed_deltas) == 2
    assert {a.metric for a in attributions} == set(ATTRIBUTION_METRICS)


# =========================================================================== #
# 6) Marginals that do not add up to the measured total.                      #
# =========================================================================== #
def _five_arm_seed(
    seed: int, *, reused: dict[str, int], tallies: dict[str, dict[str, int]],
) -> list[ArmMeasurement]:
    """One seed's five arms, given each arm's reuse count and cause tally."""
    configs = {
        BASELINE_ARM: ReuseRungConfig.all_enabled(),
        "minus_certificate_unchanged": ReuseRungConfig.without(
            RUNG_CERTIFICATE_UNCHANGED
        ),
        "minus_effect_bound": ReuseRungConfig.without(RUNG_EFFECT_BOUND),
        "minus_bounded_dominance": ReuseRungConfig.without(RUNG_BOUNDED_DOMINANCE),
        NO_RUNGS_ARM: ReuseRungConfig.none_enabled(),
    }
    return [
        _measure(seed=seed, rungs=cfg, actions_reused=reused[arm],
                 charged_units=reused[arm], cause_tally=tallies[arm])
        for arm, cfg in configs.items()
    ]


#: The recorded run's seed 0, exactly: three certificate skips that the
#: effect-bound rung re-earns when the certificate rung is ablated.
_SEED0_REUSED = {
    BASELINE_ARM: 7, "minus_certificate_unchanged": 7, "minus_effect_bound": 4,
    "minus_bounded_dominance": 6, NO_RUNGS_ARM: 0,
}
_SEED0_TALLIES = {
    BASELINE_ARM: {CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED: 3,
                   CAUSE_SKIP_JUSTIFIED: 3, CAUSE_SKIP_DOMINANCE: 1},
    "minus_certificate_unchanged": {CAUSE_SKIP_JUSTIFIED: 6,
                                    CAUSE_SKIP_DOMINANCE: 1},
    "minus_effect_bound": {CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED: 3,
                           CAUSE_SKIP_DOMINANCE: 1},
    "minus_bounded_dominance": {CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED: 3,
                                CAUSE_SKIP_JUSTIFIED: 3},
    NO_RUNGS_ARM: {},
}


def test_marginals_that_do_not_add_up_are_REPORTED_not_left_inferable() -> None:
    """Sum(one-at-a-time marginals) is not the measured total saving, and the
    shortfall is a recorded figure.

    On the recorded seed 0 the marginals are 0 / -3 / -1 = -4 while the measured
    total (all_rungs vs no_rungs) is -7. Three reuses are attributed to NO rung
    by the one-at-a-time table — exactly the three the certificate rung earned
    and the effect-bound rung re-earned."""
    report = AblationReport(measurements=tuple(
        _five_arm_seed(0, reused=_SEED0_REUSED, tallies=_SEED0_TALLIES)
    ))
    rows = {(r["seed"], r["metric"]): r for r in report.attribution_residuals()}
    row = rows[(0, "actions_reused")]
    print(f"\n[report/residual] {row}")
    assert row["per_rung_marginals"] == {
        RUNG_CERTIFICATE_UNCHANGED: 0.0, RUNG_EFFECT_BOUND: -3.0,
        RUNG_BOUNDED_DOMINANCE: -1.0,
    }
    assert row["sum_of_marginals"] == -4.0
    assert row["measured_total"] == -7.0
    assert row["residual"] == -3.0
    assert row["residual_share_of_total"] == pytest.approx(3 / 7, rel=1e-3)
    assert row["additive"] is False
    assert set(r["metric"] for r in report.attribution_residuals()) == set(
        RESIDUAL_METRICS
    )
    assert "evidence_lifetime_generations" not in RESIDUAL_METRICS, (
        "a MEAN's differences do not sum, so a residual over it is meaningless"
    )
    assert report.seeds_without_residual() == []
    # the durable JSON carries it, so a reader of the artefact alone sees it.
    stored = report.to_canonical_dict()
    assert stored["attribution_residuals"] == report.attribution_residuals()
    assert "ADDITIVITY" in report.render()

    # ...and an ADDITIVE seed reports a zero residual rather than nothing.
    additive_reused = {BASELINE_ARM: 7, "minus_certificate_unchanged": 7,
                       "minus_effect_bound": 4, "minus_bounded_dominance": 6,
                       NO_RUNGS_ARM: 3}
    additive = AblationReport(measurements=tuple(
        _five_arm_seed(1, reused=additive_reused, tallies=_SEED0_TALLIES)
    ))
    add_row = next(r for r in additive.attribution_residuals()
                   if r["metric"] == "actions_reused")
    assert add_row["residual"] == 0.0 and add_row["additive"] is True


def test_a_seed_missing_an_arm_says_so_rather_than_reporting_no_residual() -> None:
    """A seed with no residual row must be distinguishable from an additive
    one — a silent absence would read as additivity."""
    partial = AblationReport(measurements=(
        _baseline(seed=4),
        _measure(seed=4, rungs=ReuseRungConfig.without(RUNG_EFFECT_BOUND)),
    ))
    print(f"\n[report/residual-gap] {partial.seeds_without_residual()}")
    assert partial.attribution_residuals() == []
    assert partial.seeds_without_residual() == [{
        "seed": 4,
        "arms_not_run": ["minus_bounded_dominance",
                         "minus_certificate_unchanged", "no_rungs"],
        "reason": ("a residual needs the baseline, every single-rung arm and "
                   "the all-off arm"),
    }]
    assert "residual NOT MEASURED" in partial.render()


def test_a_substituted_rung_is_reported_as_redundant_not_as_unexercised() -> None:
    """THE REFUTED CLAIM. The certificate rung fires three times on seed 0 and
    its marginal is zero, because ablating it lets the effect-bound rung
    re-approve exactly those three skips. That is redundancy between rungs, not
    a rung the workload never reaches."""
    measurements = tuple(
        _five_arm_seed(0, reused=_SEED0_REUSED, tallies=_SEED0_TALLIES)
    )
    report = AblationReport(measurements=measurements,
                            attributions=attribute(measurements))
    findings = report.substitution_findings()
    print(f"\n[report/substitution] {findings}")
    assert len(findings) == 1
    (finding,) = findings
    assert finding["rung"] == RUNG_CERTIFICATE_UNCHANGED
    assert finding["gross_skips_under_complete_ladder"] == 3
    assert finding["marginal_actions_reused"] == 0
    assert finding["absorbed_by_causes"] == {CAUSE_SKIP_JUSTIFIED: 3}
    assert "REDUNDANCY" in finding["reading"]

    # the attribution row itself refuses the 'did not pay' reading.
    row = next(a for a in report.attributions
               if a.rung == RUNG_CERTIFICATE_UNCHANGED
               and a.metric == "actions_reused")
    assert row.verdict == VERDICT_REDUNDANT
    assert row.mean == 0.0
    assert row.gross_firings == {0: 3}
    assert row.to_canonical_dict()["gross_firings_total"] == 3
    assert "SUBSTITUTION" in report.render()
    assert report.to_canonical_dict()["substitution_findings"] == findings

    # a rung that genuinely never fired is NOT reported as substitution.
    silent = dict(_SEED0_TALLIES)
    silent[BASELINE_ARM] = {CAUSE_SKIP_JUSTIFIED: 6, CAUSE_SKIP_DOMINANCE: 1}
    silent["minus_certificate_unchanged"] = {CAUSE_SKIP_JUSTIFIED: 6,
                                             CAUSE_SKIP_DOMINANCE: 1}
    quiet_ms = tuple(_five_arm_seed(0, reused=_SEED0_REUSED, tallies=silent))
    quiet = AblationReport(measurements=quiet_ms,
                           attributions=attribute(quiet_ms))
    assert quiet.substitution_findings() == []
    assert next(
        a for a in quiet.attributions
        if a.rung == RUNG_CERTIFICATE_UNCHANGED and a.metric == "actions_reused"
    ).verdict == VERDICT_DID_NOT_PAY


def test_the_caveats_name_the_substitution_effect() -> None:
    """A reader of the durable JSON alone must be able to learn that a zero
    marginal may be redundancy rather than absence."""
    report = AblationReport(measurements=(_baseline(),))
    caveats = report.to_canonical_dict()["caveats"]
    text = caveats["a_zero_marginal_may_be_redundancy_not_absence"]
    print(f"\n[report/caveat] {text}")
    assert "MARGINAL is not GROSS" in text
    assert "SUBSTITUTION" in text
    assert "attribution_residuals" in text
    assert "redundant_on_this_workload" in text


def test_a_missing_baseline_arm_refuses_rather_than_defaults() -> None:
    """Without the same seed's all-rungs arm there is nothing to difference
    against. That RAISES; it never falls back to another seed's baseline."""
    report = AblationReport(measurements=(
        _measure(seed=3, rungs=ReuseRungConfig.without(RUNG_EFFECT_BOUND)),
    ))
    with pytest.raises(SubstrateError) as excinfo:
        report.baseline_for(3)
    assert "no all-rungs baseline arm" in str(excinfo.value)


# =========================================================================== #
# The fourth ablation the paper lists, and does not have.                     #
# =========================================================================== #
def test_objective_bounds_is_not_applicable_never_a_zero() -> None:
    """The paper's fourth E3 ablation is a scalar cost objective that does not
    exist. It is reported NOT APPLICABLE with its adjudication citation — never a
    zero (which would claim a measured no-effect) and never silently dropped."""
    report = AblationReport(measurements=(_baseline(),))
    stored = report.to_canonical_dict()["objective_bounds"]
    print(f"\n[report/objective-bounds] {stored}")
    assert stored["status"] == "not_applicable"
    assert stored["measured"] is False
    assert "adjudication row 9" in stored["citation"]
    assert "2026-07-13" in stored["citation"]
    # not a zero, and not absent.
    assert "value" not in stored and "units" not in stored
    assert "objective_bounds" in report.render()
    assert "not_applicable" in report.render()
    assert stored == dict(OBJECTIVE_BOUNDS_ROW)


def test_the_price_table_is_shared_and_recorded() -> None:
    """Every arm shares one price table, and the report records it — an arm that
    differs from its baseline in the price table is not an ablation."""
    report = AblationReport(measurements=(_baseline(), _measure(
        rungs=ReuseRungConfig.without(RUNG_EFFECT_BOUND))))
    stored = report.to_canonical_dict()
    assert stored["price_table"] == dict(ABLATION_PRICE_TABLE)
    assert "model_calls" not in stored["price_table"], (
        "model_calls carries MEASURED dollars joined from okg.llm_calls; a unit "
        "price for it would fabricate spend"
    )
    # every arm records its OWN corpus/spec, so a reader can check the arms
    # shared one rather than take it on trust.
    corpora = {json.dumps(m["workload"], sort_keys=True)
               for m in stored["measurements"]}
    assert len(corpora) == 1, "the arms on a seed did not share one corpus"
    for m in stored["measurements"]:
        assert m["workload"]["price_table"] == dict(ABLATION_PRICE_TABLE)
        assert m["workload"]["seed"] == m["seed"]
    assert BASELINE_ARM == "all_rungs"
