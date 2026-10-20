"""E5 adaptive replanning: the preregistration, the epoch, and the grade.

PACT ``concord-disclosure-budget``, task.batch.adaptive-replanning.
Covers :mod:`okg.bench.synth.batch_adaptive_replanning` against
``designs/batch-scheduling-research.md`` §7 (epoch closure), §9 (the
unknown-revelation refusal), §11 preamble (preregistration), §11 E5 (the
scripts, policies, metrics, and falsification clauses), and §12.

The suite is organised as the memos require: the preregistration is
pinned and cannot drift; every falsifier is checked against its FULL
preregistered text and over a nonzero denominator; the two central
clauses have vacuity tests that neuter the mechanism in scratch and
assert NAMED failures; and every conclusion refuses on an empty
denominator.
"""
from __future__ import annotations

import ast
import inspect
import json
import pathlib
import re
from dataclasses import replace

import pytest

from okg.bench.synth import batch_adaptive_replanning as e5
from okg.bench.synth.batch_adaptive_replanning import (
    CANDIDATES,
    CASES,
    CASE_EFFECT_WIDENING,
    CASE_REVEALABLE_UNKNOWN,
    CLAUSES,
    CLAUSE_DISCLOSURE_MISSES_EVENTUAL_LOCATION,
    CLAUSE_PUBLICATION_AFTER_REVEALABLE_MUST,
    CLAUSE_REPLANNING_COST_DOMINATES,
    CLAUSE_STALE_PLAN_CONTINUES_ADMISSION,
    CONCLUSION_NOT_ESTABLISHED,
    CONCLUSION_NOT_ESTABLISHED_INVALID,
    DEFERRED,
    DISCLOSURE_SCOPE_NAME,
    MINIMUM_ADMISSION_STOP_PROBES,
    MINIMUM_COST_ACCOUNTED_RUNS,
    MINIMUM_GRADED_PUBLICATIONS,
    PARTIAL_E5_DEFERRALS,
    POLICIES,
    POLICY_STAGED_BARRIERS,
    POLICY_STATIC_UNTIL_INVALIDATION,
    PREREGISTERED_MANIFEST_DIGEST,
    SCRIPTS,
    SCRIPT_LATE_MUST,
    SCRIPT_NO_REVELATION,
    STATUS_CHECKED_FEASIBLE,
    STATUS_REFUSED,
    VERDICT_PASS,
    WORKLOAD_NAMES,
    ZERO_TOLERANCE,
    AdmissionLedger,
    BatchAdaptiveReplanningError,
    E5Manifest,
    StalePlanAdmission,
    admission_stop_witness,
    build_manifest,
    build_timeline,
    cost_conclusion,
    disclosure_identity,
    eventual_locations,
    publications_with_outstanding_must,
    run_policy,
    run_report,
    uncovered_mandatory_units,
    workloads,
)
from okg.substrate.consistency.batch_schedule import (
    load_batch_instance,
    load_plan,
)
from okg.substrate.consistency.batch_schedule_checker import (
    CAUSE_MANDATORY_UNIT_UNCOVERED,
    CAUSE_MAY_CHANGE_INCOMPLETE,
    CAUSE_UNKNOWN_REVELATION,
    check_schedule,
)
from okg.substrate.consistency.batch_schedule_oracle import build_certificate
from okg.substrate.consistency.observations import (
    SCOPE_DIGEST_ALGORITHM,
    scope_digest,
    scope_identity,
)

MODULE_PATH = pathlib.Path(e5.__file__)


def _instance_deferring_everything():
    """A real instance plus a plan that defers every action.

    Deferring everything leaves the base `must` uncovered, which is the
    only shape both the checker and the harness's own re-derivation are
    supposed to refuse."""
    workload = next(w for w in workloads() if w.name == "linear_chain")
    scope = eventual_locations(workload, SCRIPT_NO_REVELATION, "none")
    instance = load_batch_instance(e5.compile_instance(
        run_key="directed", epoch=0,
        obligations=[dict(o) for o in workload.obligations],
        actions=[dict(a) for a in workload.actions], carried=[],
        observation_digest="0" * 64, affected_scope=scope))
    plan = load_plan({
        "schema_version": instance.schema_version,
        "instance_id": instance.instance_id,
        "selected": [],
        "deferred": sorted(a.id for a in instance.actions),
        "witnesses": [],
        "may_change": sorted(scope),
        "claimed_objective": {
            "should_units": 0, "may_units": 0, "total_cost": 0,
            "makespan": 0, "tie_break": ""},
    })
    return instance, plan


def _run_with_row(**overrides):
    """A minimal qualified-publication run, shaped for the detector.

    Built by hand rather than harvested from the grid so each half of the
    clause can be driven on its own: the corpus cannot produce a
    qualified epoch with an outstanding `must`, which is the result, not
    a reason to leave the detector untested."""
    row = e5.EpochRow(
        epoch=0, instance_id="directed", instance_digest="d",
        plan_digest="p", status=STATUS_CHECKED_FEASIBLE, causes=(),
        not_established=(), actions_planned=1, selected=("a",), deferred=(),
        carried_in=(), carried_out=(), carried_placed_at_zero=0,
        admitted=("a",), stale=False, stale_at=None,
        admission_stop_probes=0, admission_stop_refusals=0,
        admitted_after_stale=(), final=True, publication_qualified=True,
        publication_deferred_reason="", pending_must_revealable=(),
        uncovered_must=(), objective=None, planning_units=1, idle_ticks=0,
        grade=None)
    return e5.PolicyRun(
        workload="directed", workload_digest="w", script=SCRIPT_LATE_MUST,
        case="none", timeline_digest="t",
        policy=POLICY_STATIC_UNTIL_INVALIDATION, candidate="fifo",
        roster_version="r", manifest_digest="m",
        status=STATUS_CHECKED_FEASIBLE, detail="", epochs=(
            replace(row, **overrides),),
        planning_invocations=1, planning_units=1, executed_cost=0,
        wasted_cost=0, useful_cost=0, saved_execution=0,
        delayed_mandatory_ticks=0)


@pytest.fixture(scope="module")
def manifest() -> E5Manifest:
    return build_manifest()


@pytest.fixture(scope="module")
def report(manifest: E5Manifest):
    return run_report(manifest)


# --------------------------------------------------------------------------- #
# Preregistration (§11 preamble).                                              #
# --------------------------------------------------------------------------- #
def test_the_manifest_digest_is_pinned(manifest: E5Manifest) -> None:
    """A preregistration that binds nothing is not a preregistration.

    The pin is a literal constant in the module, so an outcome-driven edit
    to the corpus, the grid, the clause text, the metric definitions, the
    thresholds, or the deferrals must move this constant in the same
    reviewable diff."""
    assert manifest.digest() == PREREGISTERED_MANIFEST_DIGEST


def test_the_manifest_is_built_from_configuration_only() -> None:
    """No result object of any kind may shape the preregistration."""
    signature = inspect.signature(build_manifest)
    assert set(signature.parameters) == {
        "workload_names", "scripts", "cases", "policies"}
    for parameter in signature.parameters.values():
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY


def test_changing_the_configuration_moves_the_pinned_digest() -> None:
    narrowed = build_manifest(workload_names=WORKLOAD_NAMES[:1])
    assert narrowed.digest() != PREREGISTERED_MANIFEST_DIGEST


def test_a_manifest_may_not_drop_a_reveal_script() -> None:
    """§11 E5 names FOUR scripts. Three is a different experiment."""
    with pytest.raises(BatchAdaptiveReplanningError) as excinfo:
        build_manifest(scripts=SCRIPTS[:3])
    assert "FOUR reveal scripts" in str(excinfo.value)


def test_every_deferral_is_a_named_manifest_row(manifest: E5Manifest) -> None:
    """The required set is ``set(DEFERRED)``, never a hand-listed subset.

    A hand-listed subset would let a deferral be dropped and caught only
    by the pinned digest, which says a number moved but not which row
    went."""
    assert set(manifest.deferred) == set(DEFERRED)
    for name, text in manifest.deferred.items():
        assert len(text) > 80, name


def test_the_partial_verdict_deferrals_are_real_rows() -> None:
    assert set(PARTIAL_E5_DEFERRALS) <= set(DEFERRED)


def test_every_zero_tolerance_clause_carries_its_full_text(
    manifest: E5Manifest,
) -> None:
    assert set(manifest.zero_tolerance) == set(ZERO_TOLERANCE)
    for name in ZERO_TOLERANCE:
        assert manifest.zero_tolerance[name] == CLAUSES[name]



def test_the_preregistered_minimums_are_structural() -> None:
    """Each floor is derived from the grid's shape, not chosen after."""
    assert MINIMUM_ADMISSION_STOP_PROBES == len(WORKLOAD_NAMES) * len(POLICIES)
    assert MINIMUM_GRADED_PUBLICATIONS == len(WORKLOAD_NAMES) * len(CANDIDATES)
    assert MINIMUM_COST_ACCOUNTED_RUNS == (
        len(WORKLOAD_NAMES) * len(POLICIES) * len(CANDIDATES))


# --------------------------------------------------------------------------- #
# §11 E5's four reveal scripts and the case grid.                              #
# --------------------------------------------------------------------------- #
def test_all_four_design_named_reveal_scripts_are_present_and_distinct(
    manifest: E5Manifest,
) -> None:
    """Enumerated against §11 E5's own list, so a drop is visible."""
    assert manifest.reveal_scripts == SCRIPTS
    assert manifest.design_named_reveal_scripts == SCRIPTS
    assert len(set(SCRIPTS)) == 4
    assert SCRIPTS == (
        "no_revelation",
        "new_should_or_may_after_a_check",
        "late_must",
        "two_step_cascade",
    )


def test_the_four_scripts_seal_four_distinct_timelines() -> None:
    """Distinct by DIGEST, not merely by name.

    Four names over one timeline would make the script axis decoration;
    the digests are the evidence they differ."""
    workload = workloads()[0]
    digests = {script: build_timeline(workload, script, "none").digest
               for script in SCRIPTS}
    assert len(set(digests.values())) == 4
    assert build_timeline(workload, SCRIPT_NO_REVELATION, "none").events == ()
    assert len(build_timeline(workload, "two_step_cascade", "none").events) == 2
    for script in (SCRIPT_LATE_MUST, "new_should_or_may_after_a_check"):
        assert len(build_timeline(workload, script, "none").events) == 1


def test_the_case_grid_covers_the_three_cases_the_design_names() -> None:
    """§11 E5: 'Include effect widening, cost overrun, and dynamic-consumer
    cases.'"""
    for name in ("effect_widening", "cost_overrun", "dynamic_consumer"):
        assert name in CASES
    assert CASE_REVEALABLE_UNKNOWN in CASES


def test_every_timeline_event_closes_an_epoch() -> None:
    """§7 names two triggers and this corpus has no third kind of event."""
    for workload in workloads():
        for script in SCRIPTS:
            for case in CASES:
                for event in build_timeline(workload, script, case).events:
                    reveals = bool(event.reveals_obligations
                                   or event.reveals_actions)
                    invalidates = bool(event.widens or dict(event.overruns))
                    assert reveals or invalidates, (script, case, event.kind)


def test_the_corpus_declares_a_scope_that_covers_its_own_truth() -> None:
    """A change's affected scope must contain every eventual truth location.

    This is the corpus's soundness premise, and the directed mutant below
    breaks exactly it."""
    for workload in workloads():
        for script in SCRIPTS:
            for case in CASES:
                scope = set(eventual_locations(workload, script, case))
                _, actions = e5.eventual_world(workload, script, case)
                for action in actions:
                    assert set(action["truth_locations"]) <= scope


# --------------------------------------------------------------------------- #
# §7's admission stop, per policy and directed.                                #
# --------------------------------------------------------------------------- #
def test_the_admission_stop_witness_refuses() -> None:
    witness = admission_stop_witness()
    assert witness.refused
    assert witness.code == "plan_digest_stale"


def test_a_stale_plan_cannot_admit_a_not_yet_started_action() -> None:
    ledger = AdmissionLedger()
    ledger.open_epoch(epoch=0, instance_digest="i", plan_digest="p")
    ledger.admit("running", plan_digest="p", at=0)
    ledger.invalidate(at=1, reason="a revelation")
    with pytest.raises(StalePlanAdmission) as excinfo:
        ledger.admit("not_started", plan_digest="p", at=2)
    assert excinfo.value.data["code"] == "plan_digest_stale"


def test_a_plan_from_another_epoch_admits_nothing() -> None:
    ledger = AdmissionLedger()
    ledger.open_epoch(epoch=1, instance_digest="i", plan_digest="p1")
    with pytest.raises(StalePlanAdmission) as excinfo:
        ledger.admit("a", plan_digest="p0", at=0)
    assert excinfo.value.data["code"] == "plan_digest_not_live"


def test_a_non_preemptive_action_cannot_start_twice() -> None:
    ledger = AdmissionLedger()
    ledger.open_epoch(epoch=0, instance_digest="i", plan_digest="p")
    ledger.admit("a", plan_digest="p", at=0)
    with pytest.raises(StalePlanAdmission) as excinfo:
        ledger.admit("a", plan_digest="p", at=5)
    assert excinfo.value.data["code"] == "already_started"


@pytest.mark.parametrize("policy", POLICIES)
def test_each_policy_stops_admitting_old_plan_work_on_invalidation(
    policy: str, report,
) -> None:
    """§11 E5: 'All policies stop admitting old-plan work after
    invalidation.'

    Directed per policy, over that policy's OWN rows: every not-yet-started
    action of every invalidated plan was ATTEMPTED and every attempt was
    refused. The probe count is asserted nonzero, because a policy that
    never invalidated would report the same clean zero as one whose stop
    works."""
    rows = [row for run in report.runs if run.policy == policy
            for row in run.epochs]
    probes = sum(row.admission_stop_probes for row in rows)
    refusals = sum(row.admission_stop_refusals for row in rows)
    escaped = [a for row in rows for a in row.admitted_after_stale]
    assert probes > 0, policy
    assert refusals == probes, policy
    assert escaped == [], policy


def test_every_stale_row_probed_what_it_could_not_admit(report) -> None:
    """A stale epoch with unadmitted work must have probed it.

    Otherwise the clause's denominator could be inflated by rows that
    invalidated nothing."""
    stale_rows = [row for run in report.runs for row in run.epochs
                  if row.stale]
    assert stale_rows
    assert any(row.admission_stop_probes > 0 for row in stale_rows)
    for row in report.runs[0].epochs:
        if not row.stale:
            assert row.admission_stop_probes == 0


# --------------------------------------------------------------------------- #
# §8 and §9 qualification.                                                     #
# --------------------------------------------------------------------------- #
def test_the_late_must_script_defers_publication_then_qualifies(
    report,
) -> None:
    """The refusal AND the resumption, inside one run.

    §11 E5's first falsifier is that publication after a revealable
    ``must`` is deferred; the experiment must show the deferral firing and
    qualified publication RESUMING once the replan covers the revealed
    work."""
    assert report.qualification.fired
    assert report.qualification.qualified_epoch > (
        report.qualification.deferred_epoch)
    run = next(
        r for r in report.runs
        if f"{r.workload}/{r.script}/{r.case}/{r.policy}/{r.candidate}"
        == report.qualification.run_key)
    assert run.script in (SCRIPT_LATE_MUST, "two_step_cascade")
    deferred_row = run.epochs[report.qualification.deferred_epoch]
    assert not deferred_row.publication_qualified
    assert deferred_row.pending_must_revealable
    assert deferred_row.publication_deferred_reason == (
        "epoch_closed_on_invalidation")
    assert run.epochs[report.qualification.qualified_epoch].publication_qualified


def test_the_late_must_script_drives_a_checker_level_refusal(report) -> None:
    """§8's feasibility condition, refused by the INDEPENDENT CHECKER.

    A deferred action declaring a mandatory obligation class in its
    revelation makes the batch infeasible, and the refusal is the
    checker's, not this harness's."""
    refused = [
        (run, row) for run in report.runs if run.script == SCRIPT_LATE_MUST
        for row in run.epochs
        if row.status == STATUS_REFUSED
        and "deferred_may_reveal_must" in row.causes
    ]
    assert refused
    run, row = refused[0]
    assert not row.publication_qualified
    assert run.status == STATUS_REFUSED


def test_the_revealable_unknown_case_refuses_publication(report) -> None:
    """§9: an UNKNOWN revelation in the deferred closure REFUSES.

    Stricter than disclosure widening on purpose — a hidden mandatory
    obligation cannot be repaired by a warning."""
    refused = [
        row for run in report.runs if run.case == CASE_REVEALABLE_UNKNOWN
        for row in run.epochs
        if CAUSE_UNKNOWN_REVELATION in row.causes
    ]
    assert refused
    for row in refused:
        assert row.status == STATUS_REFUSED
        assert not row.publication_qualified


def test_no_qualified_publication_carries_an_outstanding_must(report) -> None:
    """Both halves of the clause, over a nonzero denominator."""
    row = next(f for f in report.falsifications
               if f.clause == CLAUSE_PUBLICATION_AFTER_REVEALABLE_MUST)
    assert row.evaluated > 0
    assert not row.fired
    published = [e for run in report.runs for e in run.epochs
                 if e.publication_qualified]
    assert published
    for epoch in published:
        assert epoch.uncovered_must == ()
        assert epoch.pending_must_revealable == ()


def test_publication_is_actually_deferred_somewhere_for_a_must(
    report,
) -> None:
    """The clause's zero must be EARNED: the deferral has to happen."""
    deferred = [
        e for run in report.runs for e in run.epochs
        if not e.publication_qualified
        and (e.pending_must_revealable or e.uncovered_must)
    ]
    assert deferred
    row = next(f for f in report.falsifications
               if f.clause == CLAUSE_PUBLICATION_AFTER_REVEALABLE_MUST)
    assert "DEFERRED publication for exactly that reason" in row.detail


def test_the_uncovered_must_check_is_re_derived_not_read_back(report) -> None:
    """The harness's own §8 re-derivation agrees with the checker.

    A second opinion is only useful if it is a second opinion, and an
    agreement observed only at zero is not evidence of one. The grid half
    of this test shows the two never disagree; the DIRECTED half below
    drives the checker to fire and asserts the re-derivation fires with
    it, so neutering the re-derivation to a constant empty set is
    visible."""
    for run in report.runs:
        for row in run.epochs:
            if row.plan_digest == "":
                continue
            checker_said = CAUSE_MANDATORY_UNIT_UNCOVERED in row.causes
            assert checker_said == bool(row.uncovered_must), (
                run.workload, run.script, run.policy, row.epoch)


def test_the_re_derivation_fires_where_the_checker_fires() -> None:
    """The directed half: a plan that defers a `must`, refused by both."""
    instance, deferring = _instance_deferring_everything()
    result = check_schedule(instance, deferring,
                            build_certificate(instance, deferring))
    assert CAUSE_MANDATORY_UNIT_UNCOVERED in result.causes()
    uncovered = uncovered_mandatory_units(instance, deferring)
    assert uncovered
    assert "o_must" in uncovered


def test_the_known_must_half_of_the_clause_can_fire() -> None:
    """§11 E5's clause must be REACHABLE on its KNOWN half.

    A qualified publication carrying an uncovered mandatory unit is
    exactly what the clause forbids; this constructs one and asserts the
    detector names it. Without this, the clause's clean zero would be
    structural rather than measured."""
    run = _run_with_row(uncovered_must=("o_must",))
    assert publications_with_outstanding_must([run])


def test_the_revealable_must_half_of_the_clause_can_fire() -> None:
    """And on its POSSIBLY REVEALABLE half, independently.

    A narrower check on the known half alone could not observe this one,
    which is why the clause text names both."""
    run = _run_with_row(pending_must_revealable=("o_rev_must",))
    assert publications_with_outstanding_must([run])


def test_both_halves_firing_drives_a_named_refutation(report) -> None:
    """Either half must be able to drive the verdict to `refuted:`."""
    for kwargs in ({"uncovered_must": ("o_must",)},
                   {"pending_must_revealable": ("o_rev_must",)}):
        broken = replace(report, runs=report.runs + (_run_with_row(**kwargs),))
        rebuilt = replace(broken, falsifications=tuple(
            replace(row, fired=bool(publications_with_outstanding_must(
                broken.runs)))
            if row.clause == CLAUSE_PUBLICATION_AFTER_REVEALABLE_MUST else row
            for row in broken.falsifications))
        assert rebuilt.verdict.startswith(
            f"refuted:{CLAUSE_PUBLICATION_AFTER_REVEALABLE_MUST}"), kwargs


def test_a_clean_qualified_row_does_not_trip_the_detector() -> None:
    """The detector must not fire on the shape it is supposed to allow."""
    assert publications_with_outstanding_must([_run_with_row()]) == ()


# --------------------------------------------------------------------------- #
# §7's carried fixed reservations, validated by the CHECKER.                   #
# --------------------------------------------------------------------------- #
def test_a_carried_running_action_appears_as_a_fixed_reservation(
    report,
) -> None:
    """Carried in with a REMAINING duration and a mandatory obligation."""
    carried = [
        (run, row) for run in report.runs for row in run.epochs
        if row.carried_in
    ]
    assert carried
    run, row = carried[0]
    for carry_id in row.carried_in:
        assert carry_id.startswith("carry:")
        assert carry_id in row.selected


def test_the_checker_refuses_a_plan_that_defers_a_carried_reservation(
) -> None:
    """The reservation is FIXED because §8 makes it mandatory.

    The harness does not grade the planner on the carry and does not
    rewrite a plan: the instance makes deferring it infeasible, and the
    INDEPENDENT CHECKER is what says so."""
    workload = next(w for w in workloads() if w.name == "linear_chain")
    run, _ = run_policy(
        workload, SCRIPT_NO_REVELATION, CASE_EFFECT_WIDENING,
        POLICY_STATIC_UNTIL_INVALIDATION, "fifo", manifest_digest="directed")
    carrying = next(row for row in run.epochs if row.carried_in)
    carry_id = carrying.carried_in[0]

    raw = e5.compile_instance(
        run_key="directed", epoch=1,
        obligations=[dict(o) for o in workload.obligations],
        actions=[dict(a) for a in workload.actions],
        carried=[{"origin": carry_id[len("carry:"):], "remaining": 1}],
        observation_digest="0" * 64,
        affected_scope=eventual_locations(
            workload, SCRIPT_NO_REVELATION, CASE_EFFECT_WIDENING))
    instance = load_batch_instance(raw)
    assert instance.action_by_id(carry_id) is not None
    assert instance.action_by_id(carry_id).duration == 1
    carry_obligation = f"o_carry:{carry_id[len('carry:'):]}"
    assert instance.obligation_by_id(carry_obligation).priority == "must"

    deferring = load_plan({
        "schema_version": instance.schema_version,
        "instance_id": instance.instance_id,
        "selected": [],
        "deferred": sorted(a.id for a in instance.actions),
        "witnesses": [],
        "may_change": sorted(eventual_locations(
            workload, SCRIPT_NO_REVELATION, CASE_EFFECT_WIDENING)),
        "claimed_objective": {
            "should_units": 0, "may_units": 0, "total_cost": 0,
            "makespan": 0, "tie_break": ""},
    })
    certificate = build_certificate(instance, deferring)
    result = check_schedule(instance, deferring, certificate)
    assert not result.accepted
    assert CAUSE_MANDATORY_UNIT_UNCOVERED in result.causes()


def test_the_next_instance_digest_binds_the_carried_observations() -> None:
    """§7: 'bind their eventual observations.'"""
    workload = next(w for w in workloads() if w.name == "linear_chain")
    scope = eventual_locations(
        workload, SCRIPT_NO_REVELATION, CASE_EFFECT_WIDENING)
    digests = set()
    for observation in ("a" * 64, "b" * 64):
        raw = e5.compile_instance(
            run_key="binding", epoch=1,
            obligations=[dict(o) for o in workload.obligations],
            actions=[dict(a) for a in workload.actions],
            carried=[{"origin": "a_repair", "remaining": 1}],
            observation_digest=observation, affected_scope=scope)
        digests.add(load_batch_instance(raw).digest)
    assert len(digests) == 2


def test_a_carried_reservation_with_no_remaining_duration_refuses() -> None:
    """A completed action is not a reservation, and zero length is not an
    interval."""
    workload = workloads()[0]
    with pytest.raises(BatchAdaptiveReplanningError) as excinfo:
        e5.compile_instance(
            run_key="k", epoch=1,
            obligations=[dict(o) for o in workload.obligations],
            actions=[dict(a) for a in workload.actions],
            carried=[{"origin": "a_repair", "remaining": 0}],
            observation_digest="0" * 64,
            affected_scope=eventual_locations(workload, "no_revelation",
                                              "none"))
    assert "COMPLETED action" in str(excinfo.value)


def test_an_instance_never_carries_the_harnesss_ground_truth() -> None:
    """``truth_locations`` is the answer sheet and never reaches a plan."""
    workload = workloads()[0]
    raw = e5.compile_instance(
        run_key="k", epoch=0,
        obligations=[dict(o) for o in workload.obligations],
        actions=[dict(a) for a in workload.actions], carried=[],
        observation_digest="0" * 64,
        affected_scope=eventual_locations(workload, "no_revelation", "none"))
    assert "truth_locations" not in json.dumps(raw)
    load_batch_instance(raw)


# --------------------------------------------------------------------------- #
# MayChange: consumed identity, sound recall, and the directed mutant.         #
# --------------------------------------------------------------------------- #
def test_the_disclosure_identity_is_the_owning_surfaces_helper() -> None:
    """Design §3: reuse the disclosure surface, add no second channel.

    E5 defines no digest algorithm, no truncation rule, and no identity
    field names of its own: the block is whatever
    ``observations.scope_identity`` returns."""
    locations = ["loc:b", "loc:a"]
    assert disclosure_identity(locations) == scope_identity(
        DISCLOSURE_SCOPE_NAME, ["loc:a", "loc:b"])
    identity = disclosure_identity(locations)
    assert identity[f"{DISCLOSURE_SCOPE_NAME}_digest_algorithm"] == (
        SCOPE_DIGEST_ALGORITHM)
    assert identity[f"{DISCLOSURE_SCOPE_NAME}_digest_sha256"] == scope_digest(
        ["loc:a", "loc:b"])


def test_the_module_defines_no_digest_algorithm_of_its_own() -> None:
    """A structural guard, not a value check: the AST must show the
    algorithm IMPORTED and assigned nowhere.

    A value assertion catches a drifted mirror once one exists; this
    catches the mirror."""
    tree = ast.parse(MODULE_PATH.read_text(encoding="utf-8"))
    imported = {
        alias.asname or alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.module == "okg.substrate.consistency.observations"
        for alias in node.names
    }
    assert {"SCOPE_DIGEST_ALGORITHM", "SCOPE_ID_LIMIT",
            "scope_identity"} <= imported
    assigned = {
        target.id
        for node in ast.walk(tree) if isinstance(node, ast.Assign)
        for target in node.targets if isinstance(target, ast.Name)
    }
    assert "SCOPE_DIGEST_ALGORITHM" not in assigned
    assert "SCOPE_ID_LIMIT" not in assigned


def test_no_second_may_change_publication_channel_is_created() -> None:
    """The disclosure SET stays in the frozen schema's ``may_change``."""
    source = MODULE_PATH.read_text(encoding="utf-8")
    assert "plan.may_change" in source
    tree = ast.parse(source)
    imported = {
        alias.asname or alias.name
        for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }
    called = {
        node.func.id for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    # The subtraction recipe is the owning surface's shape and the wrong
    # one for a containment set. It is NAMED in the deferral prose and
    # neither imported nor called.
    assert "scope_complement_recipe" not in imported
    assert "scope_complement_recipe" not in called
    assert "reader_visible_channel_binding" in DEFERRED
    assert "consistency_gate_results" in DEFERRED[
        "reader_visible_channel_binding"]


def test_may_change_recall_is_one_across_the_grid(report) -> None:
    """The zero-tolerance soundness clause, over a real denominator."""
    published = report.disclosure_published
    assert published["graded"] >= MINIMUM_GRADED_PUBLICATIONS
    assert published["misses"] == []
    hit, total = published["recall"].split("/")
    assert hit == total
    assert int(total) > 0
    assert published["contained"] == published["graded"]


def test_precision_is_reported_as_the_cost_of_conservatism(report) -> None:
    """Precision has NO threshold, and it is genuinely below one.

    A precision of exactly one everywhere would mean the widening rules
    never widened, and the recall result would be measuring nothing."""
    hit, total = report.disclosure_published["precision"].split("/")
    assert int(total) > int(hit) > 0


def test_a_directed_under_disclosure_mutant_is_caught_by_the_grade() -> None:
    """The grade catches what the CHECKER structurally cannot.

    The mutant shrinks a change's affected scope so an UNKNOWN-effect
    deferred action's widening no longer covers a location eventual
    processing changes. The checker validates containment against its own
    closure over DECLARATIONS, so it still ACCEPTS; the grade is taken
    against harness-only ground truth, so recall drops below one."""
    workload = next(w for w in workloads() if w.name == "shared_coverage")
    script, case = "new_should_or_may_after_a_check", CASE_EFFECT_WIDENING
    full = eventual_locations(workload, script, case)
    shrunk = tuple(loc for loc in full if loc != "loc:a_gate")
    assert len(shrunk) == len(full) - 1

    control, _ = run_policy(
        workload, script, case, POLICY_STATIC_UNTIL_INVALIDATION,
        "marginal_coverage_greedy", manifest_digest="directed")
    control_row = next(row for row in control.epochs
                       if row.publication_qualified)
    assert control_row.grade.missing == ()
    assert control_row.grade.contained

    mutant, _ = run_policy(
        workload, script, case, POLICY_STATIC_UNTIL_INVALIDATION,
        "marginal_coverage_greedy", manifest_digest="directed",
        affected_scope=shrunk)
    mutant_row = next(row for row in mutant.epochs
                      if row.publication_qualified)
    assert mutant_row.status == STATUS_CHECKED_FEASIBLE
    assert mutant_row.causes == ()
    assert mutant_row.grade.missing == ("loc:a_gate",)
    assert not mutant_row.grade.contained
    assert mutant_row.grade.recall_numerator < (
        mutant_row.grade.recall_denominator)


def test_the_checker_still_catches_an_under_claimed_disclosure_set() -> None:
    """The other half of the pair: the checker owns containment against
    its own closure, and it still bites."""
    workload = next(w for w in workloads() if w.name == "shared_coverage")
    raw = e5.compile_instance(
        run_key="directed", epoch=0,
        obligations=[dict(o) for o in workload.obligations],
        actions=[dict(a) for a in workload.actions], carried=[],
        observation_digest="0" * 64,
        affected_scope=eventual_locations(workload, "no_revelation", "none"))
    instance = load_batch_instance(raw)
    from okg.substrate.consistency.batch_schedule_baselines import (
        plan_marginal_coverage_greedy,
    )
    plan = plan_marginal_coverage_greedy(instance)
    assert plan.may_change
    starved = replace(plan, may_change=tuple(sorted(plan.may_change))[1:])
    certificate = build_certificate(instance, starved)
    result = check_schedule(instance, starved, certificate)
    assert not result.accepted
    assert CAUSE_MAY_CHANGE_INCOMPLETE in result.causes()


def test_the_secondary_all_epoch_grade_decides_nothing(report) -> None:
    """A mid-timeline disclosure is not a publication surface.

    It is REPORTED — an epoch that cannot see work not yet revealed will
    under-disclose against eventual processing, and that is worth showing
    — but it must not enter the clause, the verdict, or the conclusion."""
    all_checked = report.disclosure_all_checked
    assert all_checked["graded"] > report.disclosure_published["graded"]
    assert all_checked["misses"]
    row = next(f for f in report.falsifications
               if f.clause == CLAUSE_DISCLOSURE_MISSES_EVENTUAL_LOCATION)
    assert not row.fired
    assert row.evaluated == (
        report.disclosure_published["graded_with_reference"])
    assert "SECONDARY, deciding nothing" in row.detail
    assert "disclosure_grading_all_checked_epochs" in (
        build_manifest().secondary_metrics)


# --------------------------------------------------------------------------- #
# Cost accounting on a hand-computed timeline.                                 #
# --------------------------------------------------------------------------- #
def test_wasted_and_useful_work_on_a_hand_computed_timeline() -> None:
    """One trace, derived by hand from the corpus's pinned numbers.

    ``linear_chain`` under ``no_revelation`` + ``effect_widening``, planned
    FIFO (which defers nothing) on a pool of capacity 2:

    * pinned costs are a_check 3, a_repair 4, a_probe 2, a_shadow 9;
    * the sealed event fires when a_probe COMPLETES at tick 3 and names
      (a_probe, a_repair) as invalidated;
    * at tick 3 a_check (0-2) and a_probe (0-3) have completed and
      a_repair (2-4) is still RUNNING, so a_repair is CARRIED, and
      a_shadow has not started, so it is refused admission;
    * a_probe had completed, so its 2 is WASTED; a_repair was running, so
      §7 carries it, binds its eventual observation, and its 4 STANDS;
      a_shadow is not in the invalidated set, so nothing is SAVED;
    * epoch 1 plans 2 actions, epoch 0 planned 4, so planning work is 6.
    """
    workload = next(w for w in workloads() if w.name == "linear_chain")
    run, _ = run_policy(
        workload, SCRIPT_NO_REVELATION, CASE_EFFECT_WIDENING,
        POLICY_STATIC_UNTIL_INVALIDATION, "fifo", manifest_digest="directed")

    assert run.status == STATUS_CHECKED_FEASIBLE
    assert len(run.epochs) == 2
    first, second = run.epochs

    assert first.stale and first.stale_at == 3
    assert first.carried_out == ("carry:a_repair",)
    assert first.admission_stop_probes == 1
    assert first.admission_stop_refusals == 1
    assert not first.publication_qualified
    assert second.carried_in == ("carry:a_repair",)
    assert second.final and second.publication_qualified

    assert run.executed_cost == 3 + 4 + 2 + 9 == 18
    assert run.wasted_cost == 2
    assert run.useful_cost == 16
    assert run.saved_execution == 0
    assert run.planning_units == 4 + 2 == 6
    assert run.planning_cost == 6 * e5.PLANNING_COST_PER_ACTION
    assert run.replans == 1


def test_saved_execution_counts_what_the_stop_actually_prevented(
    report,
) -> None:
    """The clause's numerator must be earned somewhere."""
    assert report.cost.saved_execution > 0
    assert any(run.saved_execution > 0 for run in report.runs)


def test_useful_and_wasted_partition_executed_work(report) -> None:
    for run in report.runs:
        assert run.useful_cost + run.wasted_cost == run.executed_cost
        assert run.wasted_cost >= 0
        assert run.useful_cost >= 0


def test_the_cost_clause_reports_its_declared_conversion_and_break_even(
    report,
) -> None:
    """A declared constant must show its influence, not hide behind it."""
    row = next(f for f in report.falsifications
               if f.clause == CLAUSE_REPLANNING_COST_DOMINATES)
    assert "break-even rate" in row.detail
    assert "unreduced saved-per-planning-unit" in row.detail
    assert report.cost.saved_per_planning_unit == (
        f"{report.cost.saved_execution}/{report.cost.planning_units}")
    assert "planning_cost_conversion_is_declared_not_measured" in DEFERRED


def test_the_policies_differ_on_the_metrics_they_are_meant_to_differ_on(
    report,
) -> None:
    """The policy axis has to be a real axis.

    Three policies that produced identical numbers would make §11 E5's
    comparison decoration; the ordering asserted here is the one the
    horizons imply, not a tuned outcome."""
    by_policy = {row["policy"]: row for row in report.cost.per_policy}
    static = by_policy[POLICY_STATIC_UNTIL_INVALIDATION]
    staged = by_policy[POLICY_STAGED_BARRIERS]
    rolling = by_policy["rolling_epoch_replanning"]
    assert static["replans"] < staged["replans"] < rolling["replans"]
    assert static["planning_cost"] < rolling["planning_cost"]
    assert rolling["wasted_cost"] < static["wasted_cost"]
    assert staged["delayed_mandatory_ticks"] > 0
    assert static["delayed_mandatory_ticks"] == 0
    assert rolling["delayed_mandatory_ticks"] == 0


# --------------------------------------------------------------------------- #
# Refusal-aware denominators and the refusal to conclude.                      #
# --------------------------------------------------------------------------- #
def test_a_refusing_run_is_counted_loudly_and_named_by_the_verdict(
    report,
) -> None:
    census = report.refusal_census()
    assert census["total"] > 0
    assert f":with_refusals:{census['total']}" in report.verdict
    assert census["status:refused"] == census["total"]
    for key in ("policy:", "script:", "case:", "workload:", "candidate:"):
        assert any(name.startswith(key) for name in census)


def test_a_refusing_run_is_excluded_from_the_cost_denominator(
    report,
) -> None:
    """A refusal contributed no execution; averaging it in would make a
    refusal read as a balanced account."""
    assert report.cost.accounted < len(report.runs)
    assert report.cost.not_comparable
    assert all(reason.endswith(("no_checked_epoch", "no_epochs"))
               or ":unbound:" in reason
               for reason in report.cost.not_comparable)


def test_an_empty_cost_denominator_concludes_nothing() -> None:
    account = cost_conclusion([], minimum_accounted=MINIMUM_COST_ACCOUNTED_RUNS)
    assert not account.established
    assert account.conclusion == CONCLUSION_NOT_ESTABLISHED
    assert "NOT EVALUATED" in account.headline
    assert account.break_even_planning_cost_per_action == 0


def test_a_below_minimum_cost_denominator_refuses_to_conclude(
    report,
) -> None:
    starved = replace(report, cost=replace(
        report.cost, accounted=MINIMUM_COST_ACCOUNTED_RUNS - 1))
    assert not starved.cost.established
    assert starved.verdict.startswith("invalid:cost_denominator_below_minimum")
    assert starved.cost_conclusion() == CONCLUSION_NOT_ESTABLISHED
    fired = [f.clause for f in starved.falsifications if f.fired]
    assert CLAUSE_REPLANNING_COST_DOMINATES in fired  # the row is unchanged
    assert starved.to_canonical_dict()["cost_conclusion"] == (
        CONCLUSION_NOT_ESTABLISHED)


def test_an_invalid_verdict_publishes_no_cost_conclusion(report) -> None:
    neutered = replace(report, admission_stop=replace(
        report.admission_stop, refused=False))
    assert neutered.verdict.startswith("invalid:")
    assert neutered.cost.established
    assert neutered.cost_conclusion() == CONCLUSION_NOT_ESTABLISHED_INVALID


def test_a_below_minimum_probe_denominator_is_invalid(report) -> None:
    starved = replace(report, disclosure_published=dict(
        report.disclosure_published, admission_stop_probes=1))
    assert starved.verdict.startswith(
        "invalid:admission_stop_denominator_below_minimum")


def test_a_below_minimum_graded_denominator_is_invalid(report) -> None:
    """The floor is stated over the NON-EMPTY-reference denominator.

    Starving the total graded count alone must NOT trip the floor: the
    floor is about rows that could have been wrong, and shrinking a count
    the floor is not stated over would be the wrong lever."""
    starved = replace(report, disclosure_published=dict(
        report.disclosure_published, graded_with_reference=1))
    assert starved.verdict.startswith(
        "invalid:graded_reference_publication_denominator_below_minimum")
    untouched = replace(report, disclosure_published=dict(
        report.disclosure_published, graded=1))
    assert not untouched.verdict.startswith("invalid:")


# --------------------------------------------------------------------------- #
# Vacuity sensitivity for the two central clauses.                             #
# --------------------------------------------------------------------------- #
def test_neutering_the_admission_stop_fails_named_tests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Neuter §7's stop IN SCRATCH and confirm the guard was doing work.

    The clean zero on ``stale_plan_continues_admission`` is only evidence
    if a stop that stopped stopping would be VISIBLE. With the staleness
    branch removed, the directed witness stops refusing and the derived
    verdict names the failure — and the monkeypatch is reverted by pytest
    at teardown, so nothing here touches the shipped module."""
    original = AdmissionLedger.admit

    def neutered(self, action_id, *, plan_digest, at):
        live = self._live
        if live is not None and live.stale:
            self._started[action_id] = at        # the removed refusal
            return None
        return original(self, action_id, plan_digest=plan_digest, at=at)

    monkeypatch.setattr(AdmissionLedger, "admit", neutered)
    witness = admission_stop_witness()
    assert not witness.refused
    assert "DID NOT REFUSE" in witness.detail

    # And the harness itself becomes incoherent: an action the old plan
    # admitted after its digest went stale is admitted AGAIN by the next
    # epoch's plan, which the non-preemption guard names. A stop that
    # stopped stopping is therefore loud in two places, not silent in one.
    workload = next(w for w in workloads() if w.name == "linear_chain")
    with pytest.raises(StalePlanAdmission) as excinfo:
        run_policy(
            workload, SCRIPT_NO_REVELATION, CASE_EFFECT_WIDENING,
            POLICY_STATIC_UNTIL_INVALIDATION, "fifo",
            manifest_digest="neutered")
    assert excinfo.value.data["code"] == "already_started"


def test_the_neutered_admission_stop_drives_a_named_refutation(
    report, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The zero-tolerance clause must be REACHABLE, not structurally zero."""
    escaped = replace(report, disclosure_published=dict(
        report.disclosure_published,
        admitted_after_stale=["linear_chain/.../a_shadow"]))
    rebuilt = replace(escaped, falsifications=tuple(
        replace(row, fired=True) if row.clause ==
        CLAUSE_STALE_PLAN_CONTINUES_ADMISSION else row
        for row in escaped.falsifications))
    assert rebuilt.verdict.startswith(
        f"refuted:{CLAUSE_STALE_PLAN_CONTINUES_ADMISSION}")


def test_neutering_the_disclosure_grade_drives_a_named_refutation(
    report,
) -> None:
    """The other central clause, reached the same way."""
    missed = replace(report, disclosure_published=dict(
        report.disclosure_published, misses=["shared_coverage/...@1:loc:x"]))
    rebuilt = replace(missed, falsifications=tuple(
        replace(row, fired=True) if row.clause ==
        CLAUSE_DISCLOSURE_MISSES_EVENTUAL_LOCATION else row
        for row in missed.falsifications))
    assert rebuilt.verdict.startswith(
        f"refuted:{CLAUSE_DISCLOSURE_MISSES_EVENTUAL_LOCATION}")


def test_the_qualification_witness_going_quiet_invalidates_the_run(
    report,
) -> None:
    neutered = replace(report, qualification=replace(
        report.qualification, fired=False))
    assert neutered.verdict == "invalid:qualification_witness_did_not_fire" + (
        neutered._refusal_suffix())


def test_every_falsification_clause_is_reported_and_not_vacuous(
    report,
) -> None:
    """A clause nobody can see is not a falsifier, and a clause evaluated
    zero times is not a result."""
    assert {row.clause for row in report.falsifications} == set(CLAUSES)
    for row in report.falsifications:
        assert row.evaluated > 0, row.clause
        assert row.detail
        assert row.zero_tolerance == (row.clause in ZERO_TOLERANCE)


# --------------------------------------------------------------------------- #
# Determinism, digest hygiene, and the derived verdict.                        #
# --------------------------------------------------------------------------- #
def test_the_publication_branch_histogram_names_what_decided_nothing(
    report,
) -> None:
    """Every deciding branch is a printed row, firing or not.

    A branch that never fires must be readable off the report rather
    than inferred from a key that is not there."""
    census = report.publication_reason_census()
    assert set(census) >= {
        "qualified", "checker_refused", "epoch_closed_on_invalidation",
        "policy_barrier_epoch_not_final", "planner_refused", "total"}
    assert census["total"] == sum(len(r.epochs) for r in report.runs)
    assert (sum(v for k, v in census.items() if k != "total")
            == census["total"])
    # The branches this corpus does exercise, asserted so the histogram is
    # not a row of zeros.
    for key in ("qualified", "checker_refused",
                "epoch_closed_on_invalidation",
                "policy_barrier_epoch_not_final"):
        assert census[key] > 0, key
    assert report.to_canonical_dict()["publication_reason_census"] == dict(
        sorted(census.items()))


def test_the_report_is_byte_equal_across_runs(manifest: E5Manifest) -> None:
    first = run_report(manifest)
    second = run_report(manifest)
    assert first.canonical_json() == second.canonical_json()
    assert first.digest() == second.digest()


def test_an_injected_clock_cannot_move_the_report_digest(
    manifest: E5Manifest, report,
) -> None:
    """§11 E5 names replan latency; wall time still may not reach a digest."""
    import itertools
    ticks = itertools.count(0, 999_983)
    timed = run_report(manifest, clock=lambda: next(ticks))
    assert timed.digest() == report.digest()
    assert timed.replan_latency_ns
    assert timed.replan_latency()["status"] == "measured"
    assert report.replan_latency()["status"] == "unknown"
    # Neither the readings nor any summary of them is digested: the
    # canonical field is a constant pointer, identical in both reports.
    assert "replan_latency_ns" not in timed.to_canonical_dict()
    assert timed.to_canonical_dict()["replan_latency"] == (
        report.to_canonical_dict()["replan_latency"])
    assert "NOT DIGESTED" in timed.to_canonical_dict()["replan_latency"]


def test_an_unmeasured_replan_latency_is_unknown_never_zero(report) -> None:
    latency = report.replan_latency()
    assert latency["status"] == "unknown"
    assert "never a zero" in latency["reason"]


def test_the_report_carries_no_float(report) -> None:
    def walk(value, where="$"):
        if isinstance(value, bool):
            return
        assert not isinstance(value, float), where
        if isinstance(value, dict):
            for key, item in value.items():
                walk(item, f"{where}.{key}")
        elif isinstance(value, (list, tuple)):
            for index, item in enumerate(value):
                walk(item, f"{where}[{index}]")

    walk(report.to_canonical_dict())


def test_the_local_digest_refuses_what_the_trusted_base_refuses() -> None:
    for value in (1.5, float("nan"), float("inf"), {"a": 2.5}, [3.5]):
        with pytest.raises(BatchAdaptiveReplanningError):
            e5._local_canonical_json(value)
    with pytest.raises(BatchAdaptiveReplanningError):
        e5._local_canonical_json({"x": object()})
    with pytest.raises(BatchAdaptiveReplanningError):
        e5._local_canonical_json({"$e5_bool": "true"})


def test_the_local_digest_markers_are_mutually_distinct() -> None:
    digests = {e5._local_digest(value)
               for value in (True, False, None, "true", 1)}
    assert len(digests) == 5


def test_the_module_reads_no_clock_and_draws_no_random_value() -> None:
    source = MODULE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = {
        node.module for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    } | {
        alias.name for node in ast.walk(tree)
        if isinstance(node, ast.Import) for alias in node.names
    }
    assert "random" not in imported
    assert "time" not in imported
    assert "SynthPrf" in source


def test_a_partial_e5_run_never_reports_a_bare_pass(report) -> None:
    """What ran is not all of E5, and the verdict has to say so."""
    assert report.verdict != VERDICT_PASS
    assert not report.verdict.startswith(f"{VERDICT_PASS}:")
    cleared = replace(report, deferred={})
    stem = cleared._verdict_stem()
    assert stem in (VERDICT_PASS,) or stem.startswith(
        ("replanning_claim_rejected:", "refuted:", "invalid:"))


def test_the_verdict_is_the_claim_rejection_not_a_refutation(report) -> None:
    """§11 E5's cost clause rejects a CLAIM; it does not refute the
    mechanism, whose safety clauses are the zero-tolerance ones."""
    assert report.verdict.startswith(
        f"replanning_claim_rejected:{CLAUSE_REPLANNING_COST_DOMINATES}")
    for row in report.falsifications:
        if row.zero_tolerance:
            assert not row.fired, row.clause


def test_every_reported_qualification_is_the_checkers_own_verdict(
    report,
) -> None:
    for run in report.runs:
        for row in run.epochs:
            if row.publication_qualified:
                assert row.status == STATUS_CHECKED_FEASIBLE
                assert row.causes == ()
                assert row.final


def test_the_grid_is_large_enough_to_have_measured_something(report) -> None:
    expected = (len(WORKLOAD_NAMES) * len(SCRIPTS) * len(CASES)
                * len(POLICIES) * len(CANDIDATES))
    assert len(report.runs) == expected
    assert report.disclosure_published["graded"] >= MINIMUM_GRADED_PUBLICATIONS
    assert report.cost.accounted >= MINIMUM_COST_ACCOUNTED_RUNS
    assert report.disclosure_published["admission_stop_probes"] >= (
        MINIMUM_ADMISSION_STOP_PROBES)


def test_the_module_makes_no_epoch_enforcement_claim_about_the_contract(
) -> None:
    """§7's enforcement is this harness's, and the report must say so."""
    assert "epoch_enforcement_is_experiment_layer_only" in DEFERRED
    assert "epoch_enforcement_is_experiment_layer_only" in PARTIAL_E5_DEFERRALS
    assert "epoch_enforcement_is_experiment_layer_only" in CLAUSES[
        CLAUSE_STALE_PLAN_CONTINUES_ADMISSION]
    # §7: "G5 never crosses epochs." No optimality artifact is built and
    # no optimality check is imported or called, asserted structurally
    # rather than by scanning prose that legitimately uses the word.
    tree = ast.parse(MODULE_PATH.read_text(encoding="utf-8"))
    names = {
        alias.asname or alias.name
        for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }
    assert "check_optimality_artifact" not in names
    assert "OptimalityArtifact" not in names
    assert "OPTIMALITY_METHOD_EXHAUSTIVE" not in names
    called = {
        node.func.id for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert "check_optimality_artifact" not in called
    assert "optimality_reference" in DEFERRED


# --------------------------------------------------------------------------- #
# EWD adversarial review, resolution round. Every test below mirrors an        #
# executed repro from                                                          #
# pact/changes/concord-disclosure-budget/reports/                              #
# ewd-review-adaptive-replanning.md.                                           #
# --------------------------------------------------------------------------- #
def test_the_graded_denominator_splits_the_empty_reference_rows(
    report,
) -> None:
    """EWD-BAR-1: 540 of 684 graded publications decide nothing.

    A publication whose ``recall_denominator`` is zero is contained
    VACUOUSLY — eventual full processing leaves nothing that epoch could
    have missed — so weighing the zero-tolerance recall against all 684
    states the floor over a quantity most of which is silent. Both counts
    are pinned here at the reviewer's executed numbers, and both must be
    NAMED report fields rather than something a reader reconstructs by
    walking every EpochRow."""
    published = report.disclosure_published
    assert published["graded"] == 684
    assert published["graded_with_reference"] == 144
    assert published["graded_without_reference"] == 540
    assert (published["graded_with_reference"]
            + published["graded_without_reference"]
            == published["graded"])
    # The reviewer's denominator distribution, recomputed from the rows.
    distribution: dict[int, int] = {}
    for run in report.runs:
        for row in run.epochs:
            if row.grade is None or not row.publication_qualified:
                continue
            key = row.grade.recall_denominator
            distribution[key] = distribution.get(key, 0) + 1
    assert distribution == {0: 540, 1: 60, 2: 84}
    # And both numbers reach a reader, in the serialized report and in
    # the clause detail that states the floor.
    serialized = json.loads(report.canonical_json())
    assert serialized["disclosure_published"]["graded_with_reference"] == 144
    assert serialized["disclosure_published"]["graded_without_reference"] == 540
    row = next(f for f in report.falsifications
               if f.clause == CLAUSE_DISCLOSURE_MISSES_EVENTUAL_LOCATION)
    assert row.evaluated == 144
    assert "144 qualified publication(s) WITH a non-empty reference set" in (
        row.detail)
    assert "540 publication(s) whose reference set was EMPTY" in row.detail
    assert f"floor of {MINIMUM_GRADED_PUBLICATIONS}" in row.detail


def test_a_grid_with_no_reference_rows_refuses_to_conclude_on_recall(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """EWD-BAR-1: the reviewer's repro, as an artificial grid.

    Emptying every action's harness-only ground truth leaves every
    qualified publication with a 0/0 recall — contained, but contained by
    nothing. The old gate compared 684 against 15 and passed such a run
    with a clean recall. The floor is now stated over the NON-EMPTY
    denominator, so the run must instead REFUSE: the clause reports NOT
    EVALUATED, does not fire, and the verdict NAMES the missing
    denominator rather than publishing a soundness result nobody
    measured."""
    real = e5.eventual_world

    def _no_ground_truth(workload, script, case):
        obligations, actions = real(workload, script, case)
        return obligations, tuple(
            dict(action, truth_locations=()) for action in actions)

    monkeypatch.setattr(e5, "eventual_world", _no_ground_truth)
    # The CLAIM side must stay well formed — the frozen contract refuses
    # a change with an empty affected scope — so only the REFERENCE is
    # emptied, which is exactly the reviewer's repro.
    manifest = build_manifest()
    starved = run_report(manifest, affected_scope_override={
        name: ("loc:reference_free",) for name in manifest.workloads})
    published = starved.disclosure_published
    assert published["graded"] > MINIMUM_GRADED_PUBLICATIONS
    assert published["graded_with_reference"] == 0
    assert published["graded_without_reference"] == published["graded"]
    assert published["misses"] == []
    assert starved.verdict.startswith(
        "invalid:graded_reference_publication_denominator_below_minimum")
    row = next(f for f in starved.falsifications
               if f.clause == CLAUSE_DISCLOSURE_MISSES_EVENTUAL_LOCATION)
    assert not row.fired
    assert row.evaluated == 0
    assert row.detail.startswith("NOT EVALUATED")
    # And no substantive conclusion is published beneath the refusal.
    assert starved.cost_conclusion() in (
        CONCLUSION_NOT_ESTABLISHED, CONCLUSION_NOT_ESTABLISHED_INVALID)


def test_the_publication_rule_is_declared_stronger_than_the_literal_clause(
    manifest: E5Manifest,
) -> None:
    """EWD-BAR-2: the strengthening is NAMED, not left as a definition.

    Deferring publication at every non-final epoch is a different and
    stronger proposition than §11 E5's sentence. It is defensible on §7 —
    at a non-final epoch ``may_change`` is empty by construction while
    selected-but-incomplete actions still change locations — but it is a
    DEVIATION from the clause under test, so it gets a preregistered
    field of its own, inside the manifest digest, and both withholding
    branches are named in the clause text."""
    strengthening = manifest.publication_rule_strengthening
    assert strengthening == e5.PUBLICATION_RULE_STRENGTHENING
    assert "STRICTER THAN §11 E5 READ LITERALLY" in strengthening
    assert e5.DEFERRED_EPOCH_CLOSED_ON_INVALIDATION in strengthening
    assert e5.DEFERRED_POLICY_BARRIER in strengthening
    assert "EMPTY BY CONSTRUCTION" in strengthening
    # Preregistered: it is inside the pinned digest.
    assert manifest.digest() == PREREGISTERED_MANIFEST_DIGEST
    assert "publication_rule_strengthening" in manifest.canonical_json()
    softened = replace(manifest, publication_rule_strengthening="")
    assert softened.digest() != PREREGISTERED_MANIFEST_DIGEST
    # The clause text names BOTH branches, not only the first.
    clause = CLAUSES[CLAUSE_PUBLICATION_AFTER_REVEALABLE_MUST]
    assert e5.DEFERRED_EPOCH_CLOSED_ON_INVALIDATION in clause
    assert e5.DEFERRED_POLICY_BARRIER in clause
    assert "STRONGER THAN THE LITERAL SENTENCE" in clause
    assert "publication_rule_strengthening" in clause


def test_the_literal_reading_counterfactual_is_a_named_report_surface(
    report,
) -> None:
    """EWD-BAR-2: the evidence that the stricter rule is load-bearing.

    The reviewer's executed counterfactual, pinned. Under §11 E5 read
    literally — the checker accepts, reports no cause, no `must` is known
    uncovered and none is still revealable — the two withholding branches
    would have published 881 and 492 epochs, of which 708 and 473 carry a
    MayChange set the eventual timeline escapes. Those are the
    publications the strengthening prevents, and they are what makes it a
    safety rule rather than an unaccounted deviation."""
    census = report.literal_publication_counterfactual()
    invalidation = census[e5.DEFERRED_EPOCH_CLOSED_ON_INVALIDATION]
    barrier = census[e5.DEFERRED_POLICY_BARRIER]
    assert invalidation == {
        "rows": 1196, "would_publish": 881,
        "would_publish_with_disclosure_miss": 708}
    assert barrier == {
        "rows": 582, "would_publish": 492,
        "would_publish_with_disclosure_miss": 473}
    # A checker refusal is deferred by the literal reading too, so that
    # branch's counterfactual is empty and the branch is still emitted.
    assert census[e5.DEFERRED_CHECKER_REFUSED]["would_publish"] == 0
    assert e5.STATUS_PLANNER_REFUSED in census
    # The branch totals agree with the publication-reason census.
    reasons = report.publication_reason_census()
    assert reasons[e5.DEFERRED_POLICY_BARRIER] == barrier["rows"] == 582
    assert reasons[e5.DEFERRED_EPOCH_CLOSED_ON_INVALIDATION] == 1196
    # It travels: report field and clause detail, not a helper a reader
    # would have to call.
    serialized = json.loads(report.canonical_json())
    assert serialized["literal_publication_counterfactual"][
        e5.DEFERRED_POLICY_BARRIER]["would_publish_with_disclosure_miss"] == 473
    assert serialized["publication_rule_strengthening"] == (
        e5.PUBLICATION_RULE_STRENGTHENING)
    row = next(f for f in report.falsifications
               if f.clause == CLAUSE_PUBLICATION_AFTER_REVEALABLE_MUST)
    assert "STRONGER THAN THE LITERAL SENTENCE on two withholding" in row.detail
    for number in ("582", "881", "708", "492", "473"):
        assert number in row.detail


def test_neutering_the_final_conjunct_still_drives_a_refutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """EWD-BAR-2: the strengthening is load-bearing, proven by removing it.

    The reviewer showed 15 existing tests catch this; those stay. What is
    added here is the direct consequence, in one place: publishing at
    every checker-accepted epoch turns the run REFUTED on the disclosure
    clause, and the count of misses it lets through is the counterfactual
    census's own number rather than an independent guess."""
    real = e5.run_policy

    def _publish_at_every_accepted_epoch(*args, **kwargs):
        run, latencies = real(*args, **kwargs)
        rows = tuple(
            replace(row, publication_qualified=True,
                    publication_deferred_reason="")
            if (row.status == STATUS_CHECKED_FEASIBLE and not row.causes
                and not row.uncovered_must and not row.pending_must_revealable)
            else row
            for row in run.epochs)
        return replace(run, epochs=rows), latencies

    monkeypatch.setattr(e5, "run_policy", _publish_at_every_accepted_epoch)
    loosened = run_report(build_manifest())
    assert loosened.verdict.startswith(
        f"refuted:{CLAUSE_DISCLOSURE_MISSES_EVENTUAL_LOCATION}")
    assert loosened.disclosure_published["misses"]


def test_the_metric_labels_travel_in_the_serialized_report(report) -> None:
    """EWD-BAR-3: a structural zero must not read as a measured loss.

    Staged replanning accrues 195 delayed-mandatory ticks and the other
    two policies accrue 0 — but their zero is a fact about their horizon,
    not a result. The label that says so used to live only in the
    manifest, and ``E5Report.to_canonical_dict`` emitted neither
    ``metric_definitions`` nor ``secondary_metrics``, so a reader of the
    report saw a three-row comparison staged appeared to lose. Now both
    blocks travel, and every structural-zero row carries the label
    INLINE."""
    serialized = json.loads(report.canonical_json())
    assert "delayed_mandatory_work" in serialized["metric_definitions"]
    assert "zero by construction" in (
        serialized["metric_definitions"]["delayed_mandatory_work"])
    for name in ("break_even_planning_cost_per_action", "policy_totals",
                 "disclosure_grading_all_checked_epochs"):
        assert name in serialized["secondary_metrics"]
    for name in ("break_even_planning_cost_per_action",
                 "disclosure_grading_all_checked_epochs"):
        assert "ecides" in serialized["secondary_metrics"][name]
    assert serialized["metric_definitions"] == dict(
        build_manifest().metric_definitions)
    assert serialized["secondary_metrics"] == dict(
        build_manifest().secondary_metrics)
    # The label is inline on the row it qualifies, in the report a reader
    # actually reads.
    by_policy = {row["policy"]: row for row in report.cost.per_policy}
    serialized_policies = {
        row["policy"]: row for row in serialized["cost"]["per_policy"]}
    staged = by_policy[POLICY_STAGED_BARRIERS]
    assert staged["delayed_mandatory_ticks"] == 195
    assert staged["delayed_mandatory_ticks_zero_by_construction"] is False
    assert staged["delayed_mandatory_ticks_label"].startswith("MEASURED")
    for policy in (POLICY_STATIC_UNTIL_INVALIDATION,
                   e5.POLICY_ROLLING_EPOCHS):
        row = by_policy[policy]
        assert row["delayed_mandatory_ticks"] == 0
        assert row["delayed_mandatory_ticks_zero_by_construction"] is True
        label = row["delayed_mandatory_ticks_label"]
        assert label.startswith("ZERO BY CONSTRUCTION")
        assert "NOT a measured advantage" in label
        # And the label is in the serialized document, beside the zero.
        assert serialized_policies[policy][
            "delayed_mandatory_ticks_label"] == label
        assert ("delayed_mandatory_ticks_zero_by_construction"
                in serialized_policies[policy])


def test_the_ownership_narrative_names_not_null_keys_not_a_primary_key(
) -> None:
    """EWD-BAR-4: the docstring overstated the schema.

    ``okg.consistency_gate_results`` has a surrogate ``result_id``
    primary key; there is no unique constraint over the three named
    columns. The argument the docstring is making needs only that all
    three are NOT NULL, so it stands once corrected — but a narrative
    that misstates the schema it leans on is not evidence."""
    doc = e5.__doc__ or ""
    assert "its NOT NULL keys are" in doc
    assert "``(run_id, check_id, target_generation_id)``" in doc
    assert "PRIMARY KEY is the surrogate" in doc
    schema = (pathlib.Path(e5.__file__).parents[3]
              / "okg" / "substrate" / "db" / "schema.sql").read_text(
                  encoding="utf-8")
    assert re.search(r"result_id\s+UUID PRIMARY KEY", schema)
    body = schema.split("CREATE TABLE IF NOT EXISTS okg.consistency_gate_results")[1]
    body = body.split(");")[0]
    for column in ("run_id", "check_id", "target_generation_id"):
        line = next(ln for ln in body.splitlines()
                    if ln.strip().startswith(column + " "))
        assert "NOT NULL" in line, line
    assert "NOT NULL keys" in e5.DEFERRED["reader_visible_channel_binding"]


def test_the_dominance_conclusion_names_both_conditionalities(report) -> None:
    """EWD-BAR-5: the corpus AND the declared conversion rate.

    The token named only the corpus; the rate — a declared modelling
    choice, not a measurement — reached a reader through the headline
    alone. Both premises are now in the token, and the strength of the
    rate premise is stated outright.

    CORRECTED 2026-08-29 (audit): this test used to require the sentence
    "the verdict flips only at rate 0" and "EVERY positive rate gives the
    same sign". Both were false and this test was pinning them. The
    integer break-even floors to zero whenever saved execution is smaller
    than the planning-unit count, which says only that no INTEGER rate of
    one or more avoids dominance. On this corpus the sign reverses below a
    rate of 519/8706 - about 0.06 - and that range is the plausible one,
    since planning is arithmetic over a handful of actions while one
    action is a document extraction. The sentence must now name the exact
    threshold and must NOT claim rate-independence."""
    token = e5.CONCLUSION_REPLANNING_DOES_NOT_PAY
    assert "on_this_corpus" in token
    assert "declared_conversion_rate" in token
    assert report.cost_conclusion() == token
    assert report.cost.dominates
    assert report.cost.break_even_planning_cost_per_action == 0
    sensitivity = report.cost.rate_sensitivity
    assert "NOT rate-independence" in sensitivity
    assert "flips only at rate 0" not in sensitivity
    assert "EVERY positive rate gives the same sign" not in sensitivity
    # the sentence carries the threshold, and the threshold is the real one.
    assert f"{report.cost.break_even_rate_exact:.4f}" in sensitivity
    assert report.cost.saved_per_planning_unit in sensitivity
    # and the sign really does reverse below it: this is the claim the old
    # sentence denied, asserted rather than described.
    below = report.cost.break_even_rate_exact / 2
    assert report.cost.planning_units * below < report.cost.saved_execution, (
        "below the exact break-even, planning cost must NOT exceed saved "
        "execution - if it still does, the corrected sentence is also wrong"
    )
    assert (report.cost.planning_units * e5.PLANNING_COST_PER_ACTION
            > report.cost.saved_execution), "at the declared rate it dominates"
    serialized = json.loads(report.canonical_json())
    assert serialized["cost"]["rate_sensitivity"] == sensitivity
    row = next(f for f in report.falsifications
               if f.clause == CLAUSE_REPLANNING_COST_DOMINATES)
    assert f"CONCLUSION: {token}" in row.detail
    assert f"RATE SENSITIVITY: {sensitivity}" in row.detail


def test_the_rate_sensitivity_statement_is_derived_not_asserted() -> None:
    """EWD-BAR-5: it reads the break-even rate, in every branch.

    These accounts are hand-built rather than drawn from the corpus,
    because at the declared rate of one a dominating account can only
    have a break-even of zero. A rate premise that mattered would have to
    say so, and this pins that it would."""
    def _account(**kwargs):
        base = dict(
            accounted=MINIMUM_COST_ACCOUNTED_RUNS,
            minimum_accounted=MINIMUM_COST_ACCOUNTED_RUNS,
            planning_units=10, planning_cost=30, saved_execution=25,
            executed_cost=100, wasted_cost=10, useful_cost=90,
            per_policy=(), not_comparable=())
        base.update(kwargs)
        return e5.CostAccount(**base)

    delicate = _account()
    assert delicate.dominates
    assert delicate.break_even_planning_cost_per_action == 2
    assert "DOES depend on the declared rate" in delicate.rate_sensitivity
    robust = _account(saved_execution=5)
    assert robust.dominates
    assert robust.break_even_planning_cost_per_action == 0
    # the integer floor reads zero; the exact threshold does not, and the
    # sentence must report the exact one (audit, 2026-08-29).
    assert robust.break_even_rate_exact == 0.5
    assert "NOT rate-independence" in robust.rate_sensitivity
    assert "0.5000" in robust.rate_sensitivity
    assert "flips only at rate 0" not in robust.rate_sensitivity
    # a floored zero that is genuinely rate-independent does not exist: at
    # rate 0.4 this same account does NOT dominate, which is the whole point.
    assert robust.planning_units * 0.4 < robust.saved_execution
    paying = _account(planning_cost=5)
    assert not paying.dominates
    assert "replanning pays at the declared rate" in paying.rate_sensitivity
    unestablished = _account(accounted=0)
    assert not unestablished.established
    assert unestablished.rate_sensitivity.startswith("NOT EVALUATED")
