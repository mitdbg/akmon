"""PACT ``concord-disclosure-budget``, task.batch.e1-harness — the E1
read-set completeness stress witnesses (pure half).

The harness under test drives the REAL exact engine
(:func:`okg.substrate.consistency.reuse_scheduler.run_reuse_pass`) over a
seeded world with the six reviewed counterexample check classes, and grades
every proposed skip against an independent full-processing evaluator. The
witnesses here are the §E1 falsifiers made executable:

* a declared-complete index proposes ZERO unsafe skips (zero tolerance);
* a lexical (textual-mention) index is defeated by EVERY counterexample
  class, and each defeat is CAUGHT — so the lexical index is rejected, never
  accepted on its ordinary-case record;
* removing one dependency at a time produces detected unsafe skips —
  including for the membership terms that make absence-style skipping sound
  (design v8 §4(b));
* seeded omission-rate arms are deterministic, nested across rates, and
  their induced unsafe skips are caught;
* duplicated entities do not mint skip credit;
* the manifest is preregistered — configuration in, digest out, no result
  can shape it — and every report binds it.

DB-free: the pure half runs the engine with an in-memory certificate store.
The smoke-deployment half is witnessed in
``tests/bench/synth/test_e1_support_stress.py``.
"""
from __future__ import annotations

import json

import pytest

from okg.bench.synth.e1_support_stress import (
    CONSEQUENCE_BENIGN,
    CONSEQUENCE_RAN,
    CONSEQUENCE_UNSAFE,
    COUNTEREXAMPLE_CLASSES,
    DECL_COMPLETE,
    DECL_LEXICAL,
    DEFAULT_SEEDS,
    E1ArmResult,
    E1Report,
    E1StressError,
    INJECTION_OPS,
    OMISSION_RATES,
    OmissionPlan,
    PREREGISTERED_MANIFEST_DIGEST,
    RUNTIME_SAVED_MEASURED_BY,
    RUNTIME_SAVED_NOT_MEASURED_HERE,
    UpdateGrade,
    build_checks,
    build_manifest,
    build_world,
    run_arm,
    run_report,
    term_value,
)
from okg.substrate.consistency.reuse_scheduler import (
    CAUSE_RUN_ALWAYS,
    CAUSE_UNSTATED_CHANGE,
)
from okg.substrate.consistency.reuse_scheduler import (
    FALLBACK_CAUSES,
    SKIP_CAUSES,
)

_MANIFEST = build_manifest()


@pytest.fixture(scope="module")
def report() -> E1Report:
    """One full preregistered run, shared by the read-only witnesses."""
    return run_report(_MANIFEST)


# --------------------------------------------------------------------------- #
# Zero tolerance under a declared-complete index (the E1 falsifier).           #
# --------------------------------------------------------------------------- #
def test_complete_index_proposes_no_unsafe_skip_on_any_seed(
    report: E1Report,
) -> None:
    complete = report.complete_arms()
    assert len(complete) == len(_MANIFEST.seeds)
    for arm in complete:
        assert arm.unsafe_skips == 0, (
            f"E1 FALSIFIER FIRED: {arm.arm} proposed {arm.unsafe_skips} "
            f"unsafe skip(s) under a declared-complete index "
            f"(by class: {dict(arm.unsafe_by_class)})"
        )
        # the complete arm still reuses substantially — a harness in which
        # nothing ever skips would test nothing.
        assert arm.skips_proposed > 0
        assert arm.skip_precision == 1.0
    assert report.verdict == "pass"


def test_skip_and_run_causes_come_from_the_real_engine_taxonomy(
    report: E1Report,
) -> None:
    """Every recorded cause is a member of the scheduler's own closed
    vocabulary — the harness drives the engine, it does not restate one."""
    for arm in report.arms:
        for cause in arm.skip_cause_tally:
            assert cause in SKIP_CAUSES, (arm.arm, cause)
        for cause in arm.run_cause_tally:
            assert cause in FALLBACK_CAUSES and cause not in SKIP_CAUSES, (
                arm.arm, cause,
            )


# --------------------------------------------------------------------------- #
# The six counterexample classes defeat the lexical index — and are caught.    #
# --------------------------------------------------------------------------- #
def test_every_counterexample_class_defeats_the_lexical_index_and_is_caught(
    report: E1Report,
) -> None:
    caught = report.lexical_classes_caught()
    missing = set(COUNTEREXAMPLE_CLASSES) - caught
    assert not missing, (
        f"counterexample classes not exercised under the lexical index: "
        f"{sorted(missing)} — a lexical index passing ordinary cases but "
        "failing no counterexample would be unfalsified, not sound"
    )
    for arm in report.lexical_arms():
        assert arm.unsafe_skips > 0
        assert arm.skip_precision is not None
        assert arm.skip_precision < 1.0
    disposition = report.to_canonical_dict()["lexical_index_disposition"]
    assert disposition.startswith("rejected")


def test_lexical_ordinary_case_still_passes(report: E1Report) -> None:
    """The local check's lexical support equals its gold support, so the
    lexical index is defeated by the counterexamples, never by the ordinary
    case — which is exactly why the ordinary-case record cannot accept it."""
    for arm in report.lexical_arms():
        assert arm.unsafe_by_class.get("local", 0) == 0


# --------------------------------------------------------------------------- #
# Drop-one: remove one dependency at a time.                                   #
# --------------------------------------------------------------------------- #
def test_drop_one_sweep_detects_unsafe_omissions(report: E1Report) -> None:
    assert report.drop_one, "the sweep must enumerate declared dependencies"
    consequences = {f.consequence for f in report.drop_one}
    assert consequences <= {
        CONSEQUENCE_UNSAFE, CONSEQUENCE_RAN, CONSEQUENCE_BENIGN,
    }
    unsafe = [f for f in report.drop_one if f.consequence == CONSEQUENCE_UNSAFE]
    assert unsafe, (
        "no single-dependency omission produced a detected unsafe skip — "
        "the harness cannot demonstrate it catches what it exists to catch"
    )
    for finding in unsafe:
        assert finding.unsafe_skips > 0


def test_dropping_a_membership_term_is_detected(report: E1Report) -> None:
    """The membership term is what makes absence-style skipping sound
    (design v8 §4(b)); omitting it must surface as a detected unsafe skip."""
    membership = [
        f for f in report.drop_one if f.term.startswith("members:")
    ]
    assert membership, "the complete index must declare membership terms"
    assert any(
        f.consequence == CONSEQUENCE_UNSAFE for f in membership
    ), "omitting a membership term went undetected — the absence mechanism " \
       "is not being exercised"


# --------------------------------------------------------------------------- #
# Seeded omission rates: deterministic, nested, and caught.                    #
# --------------------------------------------------------------------------- #
def test_omission_rate_drops_are_deterministic_and_nested() -> None:
    world = build_world(DEFAULT_SEEDS[0], updates=_MANIFEST.updates_per_seed)
    checks = build_checks(world.states[0])
    previous_dropped: set[tuple[str, str]] = set()
    for rate in OMISSION_RATES:
        plan = OmissionPlan(drop_rate=rate, rate_seed=DEFAULT_SEEDS[0])
        dropped: set[tuple[str, str]] = set()
        for check in checks:
            base = check.gold_support(world.states[0])
            kept = plan.apply(check.check_id, base)
            assert kept == plan.apply(check.check_id, base), "not deterministic"
            dropped.update((check.check_id, t) for t in base - kept)
        assert previous_dropped <= dropped, (
            f"rate {rate}: the drop set is not nested — the arms would not be "
            "comparable across rates"
        )
        previous_dropped = dropped
    assert previous_dropped, "the highest rate dropped nothing on this seed"


def test_omission_rate_arms_produce_caught_unsafe_skips(
    report: E1Report,
) -> None:
    rate_arms = [a for a in report.arms if a.arm.startswith("omit-")]
    assert len(rate_arms) == len(_MANIFEST.seeds) * len(OMISSION_RATES)
    assert any(a.unsafe_skips > 0 for a in rate_arms), (
        "no omission-rate arm induced an unsafe skip on any seed — the rate "
        "arms are not stressing the index"
    )
    # every induced unsafe skip is, by construction of the grading, caught:
    # the counts ARE the catches. The report never nets them away.
    for arm in rate_arms:
        assert arm.unsafe_skips == sum(arm.unsafe_by_class.values())


# --------------------------------------------------------------------------- #
# Grading is against full processing, independently recomputed.                #
# --------------------------------------------------------------------------- #
def test_unsafe_grades_match_an_independent_recomputation() -> None:
    """Re-derive every unsafe flag from the world alone: replay the carried
    result per step (updated only on RUN grades) and compare each SKIP grade
    to a fresh gold evaluation. The engine is not consulted."""
    world = build_world(DEFAULT_SEEDS[0], updates=_MANIFEST.updates_per_seed)
    checks = build_checks(world.states[0])
    by_id = {c.check_id: c for c in checks}
    collected: list[UpdateGrade] = []
    run_arm(
        world, checks, arm="witness-lexical", index_kind=DECL_LEXICAL,
        collect=collected,
    )
    assert len(collected) == _MANIFEST.updates_per_seed

    carried = {c.check_id: c.evaluate(world.states[0]) for c in checks}
    saw_unsafe = False
    for grade in collected:
        post = world.states[grade.index + 1]
        for step in grade.steps:
            truth = by_id[step.check_id].evaluate(post)
            expected_skippable = truth == carried[step.check_id]
            assert step.gold_skippable == expected_skippable, (
                grade.index, step.check_id,
            )
            if step.action == "skip":
                assert step.unsafe == (not expected_skippable), (
                    grade.index, step.check_id,
                )
                saw_unsafe = saw_unsafe or not expected_skippable
            else:
                assert not step.unsafe
                carried[step.check_id] = truth
    assert saw_unsafe, "the lexical witness trajectory produced no unsafe skip"


# --------------------------------------------------------------------------- #
# Entity duplication does not mint credit.                                     #
# --------------------------------------------------------------------------- #
def test_duplicate_entities_do_not_mint_skip_credit() -> None:
    world = build_world(DEFAULT_SEEDS[0], updates=_MANIFEST.updates_per_seed)
    checks = build_checks(world.states[0])
    dup_indexes = [
        i for i, op in enumerate(world.operations)
        if op == "duplicate_doc_entity"
    ]
    assert dup_indexes, "the injection block guarantees a duplication update"
    collected: list[UpdateGrade] = []
    result = run_arm(
        world, checks, arm="witness-dup", index_kind=DECL_COMPLETE,
        collect=collected,
    )
    assert result.unsafe_skips == 0
    for index in dup_indexes:
        grade = collected[index]
        # one graded evaluation per CHECK — a duplicated entity adds no
        # evaluations and no skip credit (coverage counts equivalence
        # classes, never raw rows).
        assert len(grade.steps) == len(checks)
        assert not any(s.unsafe for s in grade.steps)
        ran = {s.check_id for s in grade.steps if s.action == "run"}
        # the duplication is visible exactly through membership terms.
        assert "check.absence.no-flagged-doc" in ran


# --------------------------------------------------------------------------- #
# Preregistration.                                                             #
# --------------------------------------------------------------------------- #
def test_manifest_is_preregistered_and_binds_the_report(
    report: E1Report,
) -> None:
    # the PINNED digest (review finding EWD-E1-1): an outcome-driven edit to
    # seeds, rates, rules, or thresholds fails HERE and must change the
    # pinned constant in the same reviewable diff.
    assert _MANIFEST.digest() == PREREGISTERED_MANIFEST_DIGEST
    assert report.manifest_digest == PREREGISTERED_MANIFEST_DIGEST
    for rule in (
        "unsafe_skip_under_complete_index",
        "counterexample_not_caught_under_lexical_index",
        "undetected_unsafe_outcome",
    ):
        assert rule in _MANIFEST.zero_tolerance
    assert _MANIFEST.primary_metric == "unsafe_skip_count"
    assert _MANIFEST.omission_rates == (0.01, 0.05, 0.10, 0.20)


def test_runtime_saved_is_delivered_but_not_measured_in_a_pure_arm(
    report: E1Report,
) -> None:
    """Runtime saved is now DELIVERED (task.batch.support-index-theorem), but
    it is a wall-clock quantity measured on the smoke half. A pure arm must
    say "not measured here" and name where it IS measured — never a measured
    0, and no longer a deferral."""
    assert "runtime_saved" not in _MANIFEST.deferred_metrics
    assert "runtime_saved" in _MANIFEST.delivered_metrics
    for arm in report.arms:
        row = arm.to_canonical_dict()["runtime_saved"]
        assert row["status"] == RUNTIME_SAVED_NOT_MEASURED_HERE
        assert row["measured_by"] == RUNTIME_SAVED_MEASURED_BY


def test_no_e1_metric_remains_deferred(report: E1Report) -> None:
    """§E1 names six metrics; with this task landed none is deferred. On a
    pure-only report the two live-path metrics carry honest not-measured
    markers rather than a zero or an empty clean sweep."""
    assert dict(_MANIFEST.deferred_metrics) == {}
    assert set(_MANIFEST.delivered_metrics) == {
        "runtime_saved", "checker_mutation_detection",
    }
    payload = json.loads(report.canonical_json())
    assert payload["runtime_saved"]["status"] == RUNTIME_SAVED_NOT_MEASURED_HERE
    detection = payload["checker_mutation_detection"]
    assert detection["status"] == "not_measured_in_this_report"
    assert "rows" not in detection


def test_report_is_canonically_serializable(report: E1Report) -> None:
    payload = json.loads(report.canonical_json())
    assert payload["schema"] == "okg.synth.e1-support-stress-report.v2"
    assert payload["verdict"] == "pass"
    assert payload["manifest_digest"] == _MANIFEST.digest()


# --------------------------------------------------------------------------- #
# The verdict is derived and names its failure.                                #
# --------------------------------------------------------------------------- #
def _arm(**overrides) -> E1ArmResult:
    base = dict(
        arm="x", seed=1, index_kind=DECL_COMPLETE, omission="none",
        probes="none",
        updates=1, steps_graded=7, skips_proposed=3, unsafe_skips=0,
        safe_skips=3, gold_skippable=4, runs=4, fallback_runs=0,
        run_cause_tally={}, skip_cause_tally={}, unsafe_by_class={},
        support_size_min=1, support_size_mean=2.0, support_size_max=4,
    )
    base.update(overrides)
    return E1ArmResult(**base)


def test_verdict_refuted_on_unsafe_complete_arm() -> None:
    bad = E1Report(
        manifest_digest="d",
        arms=(_arm(unsafe_skips=1, safe_skips=2,
                   unsafe_by_class={"absence": 1}),),
        drop_one=(),
    )
    assert bad.verdict == "refuted:unsafe_skip_under_complete_index"


def test_verdict_invalid_when_a_counterexample_class_is_not_exercised() -> None:
    lexical = _arm(
        arm="lex", index_kind=DECL_LEXICAL, unsafe_skips=1, safe_skips=2,
        unsafe_by_class={"absence": 1},
    )
    partial = E1Report(
        manifest_digest="d", arms=(_arm(), lexical), drop_one=(),
    )
    assert partial.verdict.startswith(
        "invalid:counterexample_class_not_exercised:"
    )
    assert "alias" in partial.verdict


def test_precision_recall_and_fallback_have_no_defaulted_denominator() -> None:
    empty = _arm(
        steps_graded=0, skips_proposed=0, safe_skips=0, unsafe_skips=0,
        gold_skippable=0, runs=0,
    )
    assert empty.skip_precision is None
    assert empty.skip_recall is None
    assert empty.fallback_rate is None


def test_fallback_probes_make_the_fallback_rate_movable(
    report: E1Report,
) -> None:
    """Review finding EWD-E1-3: the fallback-rate instrument must be able to
    read nonzero. The probe arm declares one check's envelope unbounded and
    withholds another's change statement; the engine's own fallback causes
    (run_always, unstated_change) must appear, the rate must move, and the
    probes must not corrupt soundness (zero unsafe skips — the probes force
    WORK, never reuse)."""
    fallback_arms = [a for a in report.arms if a.probes != "none"]
    assert len(fallback_arms) == len(_MANIFEST.seeds)
    for arm in fallback_arms:
        assert arm.fallback_rate is not None and arm.fallback_rate > 0.0
        assert arm.run_cause_tally.get(CAUSE_RUN_ALWAYS, 0) > 0
        assert arm.run_cause_tally.get(CAUSE_UNSTATED_CHANGE, 0) > 0
        assert arm.unsafe_skips == 0
    # the probe arm never masquerades as the declared-complete arm.
    assert not any(a.probes != "none" for a in report.complete_arms())
    # and outside the probe arm the rate reads a true zero for a reason the
    # report can name: every step carries an envelope, facts, and statement.
    for arm in report.complete_arms():
        assert arm.fallback_rate == 0.0


def test_deletion_counterexamples_are_injected_and_survive_complete_index(
    report: E1Report,
) -> None:
    """Review finding EWD-E1-4: §E1 names neighbour-closure LOSS, and absence
    must be exercised by deletion, not only growth. The injection block
    carries both deletion operations on every seed, and the complete index
    stays unsafe-free across them (membership/adjacency terms catch
    disappearance as well as appearance)."""
    assert "delete_flagged_doc" in INJECTION_OPS
    assert "remove_second_hop_edge" in INJECTION_OPS
    for seed in _MANIFEST.seeds:
        world = build_world(seed, updates=_MANIFEST.updates_per_seed)
        assert "delete_flagged_doc" in world.operations
        assert "remove_second_hop_edge" in world.operations
    for arm in report.complete_arms():
        assert arm.unsafe_skips == 0


def test_drop_one_consequence_labels_reflect_the_term_not_a_proxy() -> None:
    """A ``benign`` finding's term never changed value on the trajectory; a
    ``still_ran`` finding's term did change somewhere. Re-derived from the
    world alone."""
    from okg.bench.synth.e1_support_stress import run_drop_one_sweep

    world = build_world(DEFAULT_SEEDS[0], updates=_MANIFEST.updates_per_seed)
    checks = build_checks(world.states[0])
    findings = run_drop_one_sweep(world, checks)

    def term_changed(term: str) -> bool:
        return any(
            term_value(pre, term) != term_value(post, term)
            for pre, post in zip(world.states, world.states[1:])
        )

    for finding in findings:
        if finding.consequence == CONSEQUENCE_BENIGN:
            assert not term_changed(finding.term), finding
        elif finding.consequence == CONSEQUENCE_RAN:
            assert term_changed(finding.term), finding
            assert finding.unsafe_skips == 0


def test_world_refuses_a_trajectory_too_short_for_the_injection_block() -> None:
    with pytest.raises(E1StressError):
        build_world(1, updates=3)
    with pytest.raises(E1StressError):
        build_manifest(updates_per_seed=4)
