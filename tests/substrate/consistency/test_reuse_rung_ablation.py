"""E3 mechanism ablation — each reuse rung disabled ALONE, proved on the tally.

PACT ``concord-e3-mechanism-ablations``, task.e3.disable-switches
(req.e3.rungs-independently-disablable). The paper's E3 asks which of the three
shipped reuse mechanisms earns the saving, and prescribes the method: rerun one
workload with ONE rung disabled at a time. These are the witnesses that the
switch is real.

Every assertion here is on the RECORDED CAUSE TALLY
(:meth:`ReusePassResult.cause_counts`), never on the configuration flag. A switch
that is SET but does not change behaviour passes a flag assertion and fails
these — which is precisely the failure the acceptance exists to catch.

The shared ladder workload (:func:`_ladder_workload`) is one pass over four steps
chosen so that ALL THREE rungs fire in the complete-ladder arm:

* ``unchanged`` — dependency facts byte-identical to the prior generation and a
  truthful ``nothing_changed()`` statement -> the unchanged-inputs certificate;
* ``disjoint`` — a changed bucket the step's possible effects cannot reach ->
  the effect-bound rung;
* ``crawl`` — a changed bucket that DOES overlap the incumbent, with a bounded
  envelope whose every permitted output loses on declared source authority ->
  the bounded-dominance rung;
* ``overlap`` — the same overlap with NO dominance input -> a RUN in every arm
  (the control that shows an arm has not simply stopped deciding).

The arms are the complete ladder, the three single-rung ablations, and the
degenerate all-off arm that bounds the total saving from above.
"""
from __future__ import annotations

from typing import Any

import pytest

from okg.substrate.consistency.possible_effects import ChangedStateTerms
from okg.substrate.consistency.reuse_certificates import (
    RESOLUTION_V1_FACT_COVERAGE,
    CertificateDependencies,
    ReuseCertificate,
)
from okg.substrate.consistency.reuse_scheduler import (
    ACTION_RUN,
    ACTION_SKIP,
    CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED,
    CAUSE_CERTIFICATE_VALID,
    CAUSE_EFFECTS_OVERLAP,
    CAUSE_RUNG_DISABLED,
    CAUSE_SKIP_DOMINANCE,
    CAUSE_SKIP_JUSTIFIED,
    COMPARISON_KIND_RESOLUTION,
    REUSE_RUNGS,
    RUNG_BOUNDED_DOMINANCE,
    RUNG_CERTIFICATE_UNCHANGED,
    RUNG_EFFECT_BOUND,
    RUNG_SKIP_CAUSE,
    SKIP_CAUSES,
    TERMINAL_FIXED_POINT,
    DominanceInput,
    IncumbentDecision,
    ReusePass,
    ReuseRungConfig,
    ReuseStep,
    derive_competitor_bound,
    run_reuse_pass,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.producer_authority import normalize_write_envelope
from okg.substrate.telemetry.fallback_taxonomy import classify_reason

_DEPLOYMENT = "consistency-fixture"
_PREDICATE = "preferred_target"
_SOURCE_AUTHORITY_BASIS = "declared_source_authority"

_STEP_UNCHANGED = "unchanged"
_STEP_DISJOINT = "disjoint"
_STEP_CRAWL = "crawl"
_STEP_OVERLAP = "overlap"
_ALL_STEPS = (_STEP_UNCHANGED, _STEP_DISJOINT, _STEP_CRAWL, _STEP_OVERLAP)


# --------------------------------------------------------------------------- #
# In-memory capture connection (the test_reuse_scheduler idiom).               #
# --------------------------------------------------------------------------- #
class _CaptureCursor:
    def __init__(self, sink: list[tuple[Any, ...]]) -> None:
        self._sink = sink

    def __enter__(self) -> "_CaptureCursor":
        return self

    def __exit__(self, *_a: Any) -> bool:
        return False

    def execute(self, sql: str, params: Any = None) -> None:
        self._sink.append((sql, params))

    def fetchone(self) -> Any:
        return None


class _CaptureConn:
    def __init__(self) -> None:
        self.rows: list[tuple[Any, ...]] = []

    def cursor(self) -> _CaptureCursor:
        return _CaptureCursor(self.rows)

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass


# --------------------------------------------------------------------------- #
# The shared workload — ONE corpus every arm runs over.                         #
# --------------------------------------------------------------------------- #
def _facts(tag: str) -> CertificateDependencies:
    return CertificateDependencies(
        input_digests={"in": f"digest:{tag}"},
        operator_versions={"op": "v1"},
        model_prompt_keys={},
        envelope_fingerprint=f"env:{tag}",
        constraint_set={"chk": "cs:1"},
        generator_versions={"gen": "v1"},
        policy_fingerprint="pol:1",
        precedence_fingerprint="prec:1",
        competitor_bounds={"bound": tag},
        catalog_version_id="catver:1",
    )


def _bounded_envelope(key: str, source_name: str) -> Any:
    return normalize_write_envelope(
        {"nodes": [{"subtype": "consistency_claim", "clause_id": "c"}],
         "claim_keys": [key]},
        source_name=source_name,
    )


def _dominance_envelope(key: str, *, authority_max: int) -> Any:
    """A claim envelope whose closed source-authority vocabulary CEILS every
    permitted output below the incumbent — the only shape dominance can prove."""
    return normalize_write_envelope(
        {"nodes": [{"subtype": "consistency_claim", "clause_id": "c"}],
         "claim_keys": [key],
         "value_vocabularies": {"source_class": ["crawl"],
                                "source_authority": [str(authority_max)]}},
        source_name="crawl",
    )


def _ladder_workload() -> dict[str, Any]:
    """The ONE workload every arm runs: same steps, same seed order, same change
    statements, same dependency facts, same incumbents. Only the rung config
    differs between arms — an ablation whose workload was regenerated is not an
    ablation."""
    crawl_envelope = _dominance_envelope("k1", authority_max=0)
    steps = {
        _STEP_UNCHANGED: ReuseStep(
            step_id=_STEP_UNCHANGED, envelope=_bounded_envelope("k1", "u"),
            producer_id=_STEP_UNCHANGED, claim_predicates=(_PREDICATE,),
        ),
        _STEP_DISJOINT: ReuseStep(
            step_id=_STEP_DISJOINT, envelope=_bounded_envelope("k1", "d"),
            producer_id=_STEP_DISJOINT, claim_predicates=(_PREDICATE,),
        ),
        _STEP_CRAWL: ReuseStep(
            step_id=_STEP_CRAWL, envelope=crawl_envelope,
            producer_id=_STEP_CRAWL, claim_predicates=(_PREDICATE,),
        ),
        _STEP_OVERLAP: ReuseStep(
            step_id=_STEP_OVERLAP, envelope=_bounded_envelope("k1", "o"),
            producer_id=_STEP_OVERLAP, claim_predicates=(_PREDICATE,),
        ),
    }
    facts = {step_id: _facts(step_id) for step_id in steps}
    incumbent = {
        _STEP_UNCHANGED: {"outcome": "carry_forward", "basis": "unchanged_inputs"},
        _STEP_DISJOINT: {"outcome": "selected", "basis": _SOURCE_AUTHORITY_BASIS},
        _STEP_CRAWL: {"outcome": "selected", "basis": _SOURCE_AUTHORITY_BASIS},
        _STEP_OVERLAP: {"outcome": "selected", "basis": _SOURCE_AUTHORITY_BASIS},
    }
    return {
        "deployment": _DEPLOYMENT,
        "generation_id": "7",
        "worklist_seed": list(_ALL_STEPS),
        "steps": steps,
        "checks": (),
        "current_facts": facts,
        # only the unchanged step carries a byte-identical prior snapshot.
        "prior_facts": {_STEP_UNCHANGED: facts[_STEP_UNCHANGED]},
        "prior_generation_id": "6",
        "changed_terms": {
            # a TRUTHFUL "nothing changed" statement, not an omission.
            _STEP_UNCHANGED: ChangedStateTerms.nothing_changed(),
            _STEP_DISJOINT: ChangedStateTerms(
                buckets=frozenset({(_PREDICATE, "k9")})
            ),
            _STEP_CRAWL: ChangedStateTerms(buckets=frozenset({(_PREDICATE, "k1")})),
            _STEP_OVERLAP: ChangedStateTerms(buckets=frozenset({(_PREDICATE, "k1")})),
        },
        "incumbent_selection": incumbent,
        "dominance_inputs": {
            _STEP_CRAWL: DominanceInput(
                incumbents={
                    (_PREDICATE, "k1"): IncumbentDecision(
                        bucket=(_PREDICATE, "k1"), outcome="selected",
                        basis=_SOURCE_AUTHORITY_BASIS, authority=20,
                    )
                },
                competitor_bound=derive_competitor_bound(crawl_envelope),
            )
        },
    }


def _valid_comparison() -> dict[str, Any]:
    """A comparison the certificate validator ACCEPTS, so a refusal in the tests
    below can only be the rung backstop and never a malformed payload."""
    return {
        "kind": COMPARISON_KIND_RESOLUTION,
        "incumbent_outcome": "selected",
        "incumbent_basis": _SOURCE_AUTHORITY_BASIS,
        "effects_summary_digest": "effects:prior",
        "competitor_bound": {"kind": "bounded_dominance"},
        "tie_break": None,
        "fact_coverage": dict(RESOLUTION_V1_FACT_COVERAGE),
    }


def _run_arm(rungs: ReuseRungConfig) -> Any:
    return run_reuse_pass(_CaptureConn(), rungs=rungs, **_ladder_workload())


_ARMS: tuple[ReuseRungConfig, ...] = (
    ReuseRungConfig.all_enabled(),
    ReuseRungConfig.without(RUNG_CERTIFICATE_UNCHANGED),
    ReuseRungConfig.without(RUNG_EFFECT_BOUND),
    ReuseRungConfig.without(RUNG_BOUNDED_DOMINANCE),
    ReuseRungConfig.none_enabled(),
)


# =========================================================================== #
# The complete ladder reaches all three rungs (the workload is the right one). #
# =========================================================================== #
def test_complete_ladder_reaches_every_rung() -> None:
    """The shared workload fires ALL THREE rungs under the complete ladder.

    Without this the per-rung ablations would be vacuous: disabling a rung that
    never fired proves nothing."""
    result = _run_arm(ReuseRungConfig.all_enabled())
    counts = result.cause_counts()
    print(f"\n[e3/all-rungs] terminal={result.terminal} counts={counts}")

    assert result.terminal == TERMINAL_FIXED_POINT
    for rung in REUSE_RUNGS:
        cause = RUNG_SKIP_CAUSE[rung]
        assert counts.get(cause, 0) >= 1, (
            f"rung {rung} did not fire on the shared workload (cause {cause}); "
            f"the ablation of it would be vacuous. counts={counts}"
        )
    # the control step runs in every arm.
    assert counts.get(CAUSE_EFFECTS_OVERLAP, 0) == 1
    assert len(result.skips) == 3 and len(result.runs) == 1


# =========================================================================== #
# ACCEPTANCE: with a rung disabled, NO skip carries that rung's cause;         #
#             every OTHER rung still approves at least one skip.               #
# =========================================================================== #
@pytest.mark.parametrize("ablated", REUSE_RUNGS)
def test_disabling_one_rung_removes_its_cause_and_leaves_the_others(
    ablated: str,
) -> None:
    """Disabling one rung removes exactly that rung's skips from the tally and
    leaves the remaining rungs REACHABLE — a removal of one mechanism, never a
    collapse of the ladder (req.e3.rungs-independently-disablable)."""
    rungs = ReuseRungConfig.without(ablated)
    result = _run_arm(rungs)
    counts = result.cause_counts()
    print(f"\n[e3/{rungs.arm_label}] terminal={result.terminal} counts={counts}")
    for o in sorted(result.outcomes, key=lambda o: o.step_id):
        print(f"[e3/{rungs.arm_label}]   {o.step_id}: {o.action}/{o.cause}")

    assert result.terminal == TERMINAL_FIXED_POINT
    # -- the ablation itself, asserted on the TALLY, never on the flag ---------
    ablated_cause = RUNG_SKIP_CAUSE[ablated]
    assert counts.get(ablated_cause, 0) == 0, (
        f"{ablated} is disabled yet {counts[ablated_cause]} skip(s) still carry "
        f"its cause {ablated_cause!r} — the switch changed no behaviour"
    )
    assert not [o for o in result.skips if o.cause == ablated_cause]
    assert result.rung_ablation_violations() == {}

    # -- the remaining rungs are still REACHABLE ------------------------------
    for other in REUSE_RUNGS:
        if other == ablated:
            continue
        other_cause = RUNG_SKIP_CAUSE[other]
        assert counts.get(other_cause, 0) >= 1, (
            f"ablating {ablated} collapsed the ladder: rung {other} approved no "
            f"skip (cause {other_cause}). counts={counts}"
        )

    # -- every step is still DECIDED (the pass did not stop deciding) ---------
    assert {o.step_id for o in result.outcomes} == set(_ALL_STEPS)


# =========================================================================== #
# ACCEPTANCE: the all-off arm terminates, skips nothing, bounds the saving.    #
# =========================================================================== #
def test_all_rungs_off_terminates_with_zero_skips() -> None:
    """With all three rungs disabled the pass still reaches a fixed point and
    still decides every step — it just proves nothing skippable. That degenerate
    arm bounds the total saving from above."""
    result = _run_arm(ReuseRungConfig.none_enabled())
    counts = result.cause_counts()
    print(f"\n[e3/no-rungs] terminal={result.terminal} counts={counts}")

    assert result.terminal == TERMINAL_FIXED_POINT
    assert result.skips == ()
    assert {o.step_id for o in result.runs} == set(_ALL_STEPS)
    assert not (set(counts) & SKIP_CAUSES), counts
    assert result.rung_ablation_violations() == {}


# =========================================================================== #
# A disabled rung's suppression is LABELLED, never mislabelled as a refusal.   #
# =========================================================================== #
def test_suppressed_effect_bound_skip_is_not_reported_as_an_overlap() -> None:
    """With the effect-bound rung off, the step whose reach was proven DISJOINT
    runs under cause ``rung_disabled`` — not ``effects_overlap``. Reporting an
    overlap the analysis never proved would corrupt the very tally E3 reads."""
    result = _run_arm(ReuseRungConfig.without(RUNG_EFFECT_BOUND))
    by_step = {o.step_id: o for o in result.outcomes}
    suppressed = by_step[_STEP_DISJOINT]
    print(f"\n[e3/label] {suppressed.step_id}: {suppressed.action}/"
          f"{suppressed.cause} detail={dict(suppressed.detail)}")

    assert suppressed.action == ACTION_RUN
    assert suppressed.cause == CAUSE_RUNG_DISABLED
    assert suppressed.detail["rung_disabled"] == RUNG_EFFECT_BOUND
    assert suppressed.detail["would_have_skipped_with_cause"] == CAUSE_SKIP_JUSTIFIED
    # the genuine overlap keeps its truthful cause in the same pass.
    assert by_step[_STEP_OVERLAP].cause == CAUSE_EFFECTS_OVERLAP
    # the new cause is TOTAL over the closed accounting taxonomy (a recorded
    # reason with no taxonomy member raises there).
    assert classify_reason(CAUSE_RUNG_DISABLED) == "operational_gate_failure"


def test_dominance_ablation_keeps_the_truthful_overlap_cause() -> None:
    """With bounded-dominance off the rung is not consulted at all: the crawl step
    falls through to the ``effects_overlap`` cause the effect-bound rung already
    established. The disabled rung proves nothing and is charged no analysis."""
    result = _run_arm(ReuseRungConfig.without(RUNG_BOUNDED_DOMINANCE))
    by_step = {o.step_id: o for o in result.outcomes}
    crawl = by_step[_STEP_CRAWL]
    print(f"\n[e3/dom-off] {crawl.step_id}: {crawl.action}/{crawl.cause} "
          f"refusal={crawl.detail.get('refusal_reason')}")
    assert crawl.action == ACTION_RUN
    assert crawl.cause == CAUSE_EFFECTS_OVERLAP
    assert crawl.detail["refusal_reason"] == "bucket_overlap"
    assert "rung_disabled" not in crawl.detail


# =========================================================================== #
# The active set is RECORDED with the run, not re-derived from the code.       #
# =========================================================================== #
def test_the_active_rung_set_is_recorded_on_the_result() -> None:
    """A reader of a recorded ablation can tell which rungs were active from the
    record alone. The canonical dict names the arm, the active set, the disabled
    set and the causes that must be absent; the digest separates the arms."""
    digests: dict[str, str] = {}
    for rungs in _ARMS:
        result = _run_arm(rungs)
        recorded = result.to_canonical_dict()["rungs"]
        print(f"\n[e3/record] {recorded}")
        assert recorded["arm"] == rungs.arm_label
        assert recorded["rungs_active"] == sorted(rungs.active)
        assert recorded["rungs_disabled"] == sorted(rungs.disabled)
        assert recorded["suppressed_skip_causes"] == sorted(
            rungs.suppressed_skip_causes()
        )
        assert result.rungs == rungs
        digests[rungs.arm_label] = result.digest()

    # five arms, five distinct digests: identical inputs under different rung
    # configurations can never collide.
    assert len(set(digests.values())) == len(_ARMS), digests
    # determinism WITHIN an arm is preserved (the DBOS-resume property).
    again = _run_arm(ReuseRungConfig.without(RUNG_EFFECT_BOUND))
    assert again.digest() == digests["minus_effect_bound"]


def test_an_unknown_rung_name_is_refused_not_ignored() -> None:
    """A typo'd rung name RAISES. Silently dropping it would run the complete
    ladder and report the run as an ablation arm — a result that looks successful
    and proves nothing."""
    with pytest.raises(SubstrateError) as excinfo:
        ReuseRungConfig(active=frozenset({"objective_bounds"}))
    assert "unknown reuse rung" in str(excinfo.value)
    with pytest.raises(SubstrateError):
        ReuseRungConfig.without("objective_bounds")
    with pytest.raises(SubstrateError):
        ReuseRungConfig.all_enabled().enabled("objective_bounds")


def test_a_disabled_rung_cannot_reach_the_skip_mechanism() -> None:
    """The structural backstop: even called DIRECTLY, the one skip mechanism
    refuses a disabled rung's cause. The ablation holds because the mechanism is
    unreachable, not because each call site remembered its gate."""
    reuse_pass = ReusePass(
        _CaptureConn(), deployment=_DEPLOYMENT, generation_id="7",
        worklist_seed=[_STEP_CRAWL], steps=_ladder_workload()["steps"],
        rungs=ReuseRungConfig.without(RUNG_BOUNDED_DOMINANCE),
    )
    with pytest.raises(SubstrateError) as excinfo:
        reuse_pass._record_skip(
            _STEP_CRAWL, _valid_comparison(), _facts(_STEP_CRAWL),
            CAUSE_SKIP_DOMINANCE, {},
        )
    print(f"\n[e3/backstop] {str(excinfo.value)[:120]}")
    assert "disabled reuse rung" in str(excinfo.value)


# =========================================================================== #
# The BOUNDARY of the switch, stated with a witness rather than left open.     #
# =========================================================================== #
def test_a_prior_certificate_still_revalidates_under_an_ablation() -> None:
    """Revalidating a still-valid PRIOR certificate is deliberately not a rung, so
    a certificate an earlier generation's rung earned still licenses a skip under
    an arm that disables that rung (cause ``certificate_valid``, not the rung's
    own cause).

    This is the ablation's one carry-over channel, recorded here with a witness so
    it is a KNOWN boundary rather than an unexamined hole: an arm must therefore
    start from a state holding no certificate a disabled rung wrote. The live
    harness gives every arm a fresh database, which discharges it by construction.
    """
    workload = _ladder_workload()
    facts = workload["current_facts"][_STEP_CRAWL]
    # a certificate the DOMINANCE rung earned in generation 6.
    prior = ReuseCertificate(
        deployment=_DEPLOYMENT, generation_id="6", step_id=_STEP_CRAWL,
        comparison=_valid_comparison(),
        dependencies=facts,
    )
    workload["prior_certificates"] = {_STEP_CRAWL: prior}
    result = run_reuse_pass(
        _CaptureConn(),
        rungs=ReuseRungConfig.without(RUNG_BOUNDED_DOMINANCE),
        **workload,
    )
    crawl = {o.step_id: o for o in result.outcomes}[_STEP_CRAWL]
    print(f"\n[e3/boundary] {crawl.step_id}: {crawl.action}/{crawl.cause}")
    assert crawl.action == ACTION_SKIP
    # NOT the disabled rung's cause — the ablation acceptance still holds...
    assert crawl.cause == CAUSE_CERTIFICATE_VALID
    assert crawl.cause != CAUSE_SKIP_DOMINANCE
    assert result.rung_ablation_violations() == {}
    # ...and the carry-over is visible in the tally as certificate_valid, so an
    # arm that inherits prior certificates reports the channel rather than hiding
    # the saving inside a rung it claims to have removed.
    assert result.cause_counts()[CAUSE_CERTIFICATE_VALID] == 1


def test_the_unchanged_rung_migrates_rather_than_disappears() -> None:
    """A recorded FINDING, not a defect: on a workload whose caller truthfully
    states ``nothing_changed()``, the skip the unchanged-inputs rung earned is
    re-earned by the effect-bound rung when that rung is ablated (an empty change
    set is trivially disjoint from any reach).

    The rung is EXERCISED — it approves the skip under the complete ladder — and
    its MARGINAL contribution is nevertheless zero, because another rung covers
    the same skip. Redundancy between rungs, not a rung the workload never
    reaches; the harness must be able to report that distinction rather than
    collapsing it into 'this mechanism did not pay'."""
    complete = _run_arm(ReuseRungConfig.all_enabled())
    ablated = _run_arm(ReuseRungConfig.without(RUNG_CERTIFICATE_UNCHANGED))
    print(f"\n[e3/migration] complete={complete.cause_counts()}")
    print(f"[e3/migration] minus_certificate_unchanged={ablated.cause_counts()}")

    assert complete.cause_counts()[CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED] == 1
    assert ablated.cause_counts().get(CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED, 0) == 0
    # the step still SKIPS — under the effect-bound rung instead.
    unchanged = {o.step_id: o for o in ablated.outcomes}[_STEP_UNCHANGED]
    assert unchanged.action == ACTION_SKIP
    assert unchanged.cause == CAUSE_SKIP_JUSTIFIED
    # so the total skip count is unchanged: this rung bought nothing HERE.
    assert len(complete.skips) == len(ablated.skips) == 3
