"""Witnesses for the E4 named-capacity and lane replay.

PACT ``concord-disclosure-budget``, task.batch.resource-replay. These
tests carry the claims the module makes about itself: that its
preregistration is pinned, that its lane compilation changes nothing
about the workload it compiles, that its borrowing arm really borrows,
that its capacity-event discipline is exercised by a breakpoint no
start-and-end scan could see, that its starvation probe fires on a
starving plan and stays quiet on an honest delay, and that its
capacity-matched comparison is computed rather than authored.

Where a check could pass vacuously, its premise is asserted beside it:
an ``evaluated > 0``, a probe count, or a re-derivation of the same
number from the outcomes.
"""
from __future__ import annotations

import inspect
import json
from dataclasses import replace
from fractions import Fraction

import pytest

from okg.bench.synth import batch_resource_replay as replay
from okg.bench.synth.batch_resource_replay import (
    BORROWING_OFF,
    BORROWING_ON_ASYMMETRIC,
    CANDIDATES,
    CLAUSES,
    CLAUSE_CAPACITY_MATCHED_COMPARISON,
    CLAUSE_LANE_EXCLUDES_MANDATORY_CLOSURE,
    CLAUSE_MANDATORY_PREREQUISITE_INVERSION,
    CLAUSE_MANDATORY_PREREQUISITE_STARVED,
    COMPARISON_BINDING_FIELDS,
    COMPARISON_STAGES,
    CONCLUSION_HARDCODING_SUPPORTED,
    CONCLUSION_NOT_ESTABLISHED,
    CONCLUSION_NOT_ESTABLISHED_INVALID,
    CONCLUSION_NOT_HARDCODED,
    COST_UNIT,
    DEFERRED,
    DESIGN_NAMED_CONFIGURATIONS,
    KIND_LANE,
    KIND_SHARED,
    LANE_CONFIG_EXCLUDES_MANDATORY_CLOSURE,
    LANE_CONFIG_VALID,
    LANE_CRITICAL,
    LANE_LIGHT,
    LANE_RULE_ACTION_CLASS,
    LANE_RULE_MANDATORY_CLOSURE,
    LANE_RULE_NONE,
    MINIMUM_COMPARABLE_PAIRS,
    PARTIAL_E4_DEFERRALS,
    PREREGISTERED_MANIFEST_DIGEST,
    STARVATION_THRESHOLD,
    STATUS_CHECKED_FEASIBLE,
    UNIT_MAPPING_VERSION,
    VERDICT_PASS,
    VERDICT_PASS_PARTIAL_E4,
    VERDICT_REFUSALS_SUFFIX,
    ZERO_TOLERANCE,
    E4Manifest,
    ResourceConfig,
    build_manifest,
    capacity_event_witness,
    capacity_matched_pairs,
    compile_instance,
    configurations,
    hand_built_workloads,
    inversion_starved_actions,
    lane_assignment,
    lane_config_validity,
    mandatory_closure,
    run_report,
    single_move_starved_actions,
    workloads,
)
from okg.substrate.consistency.batch_schedule import (
    ARTIFACT_UNBOUND,
    CONTRACT_SCHEMA_VERSION,
    PRIORITY_MUST,
    PRIORITY_SHOULD,
    ObjectiveTuple,
    load_batch_instance,
    objective_sort_key,
)
from okg.substrate.consistency.batch_schedule_checker import (
    CAUSE_CAPACITY_EXCEEDED,
    CAUSE_CONFLICT_GROUP_VIOLATION,
    check_schedule,
)
from okg.substrate.consistency.batch_schedule_oracle import build_certificate


#: §11 E4's configuration list, transcribed HERE from the design text
#: rather than imported, so the module's own tuple is compared against an
#: independent copy. If the two ever disagree, one of them moved.
DESIGN_TEXT_CONFIGURATIONS = (
    # "one shared pool with capacities 1, 2, and 4"
    "shared_pool_1",
    "shared_pool_2",
    "shared_pool_4",
    # "separate aliases (critical, light) = (1,1), (1,2), (2,2), (2,4)"
    # "and the same split configurations with borrowing disabled and enabled"
    "lane_1_1_borrow_off",
    "lane_1_2_borrow_off",
    "lane_2_2_borrow_off",
    "lane_2_4_borrow_off",
    "lane_1_1_borrow_on_asymmetric",
    "lane_1_2_borrow_on_asymmetric",
    "lane_2_2_borrow_on_asymmetric",
    "lane_2_4_borrow_on_asymmetric",
    # "a heterogeneous control such as {cpu: 4, db: 1, model: 1} versus
    #  {cpu: 4, db: 2, model: 1}"
    "heterogeneous_cpu4_db1_model1",
    "heterogeneous_cpu4_db2_model1",
)


@pytest.fixture(scope="module")
def manifest() -> E4Manifest:
    return build_manifest()


@pytest.fixture(scope="module")
def report(manifest: E4Manifest):
    return run_report(manifest)


def _check(instance, plan, candidate: str = "fifo"):
    certificate = build_certificate(
        instance, plan, producer=replay._producer(candidate),
        optimality_artifact_digest=ARTIFACT_UNBOUND)
    return check_schedule(instance, plan, certificate)


# --------------------------------------------------------------------------- #
# Preregistration.                                                             #
# --------------------------------------------------------------------------- #
def test_the_manifest_digest_is_pinned(manifest: E4Manifest) -> None:
    assert manifest.digest() == PREREGISTERED_MANIFEST_DIGEST


def test_the_manifest_is_built_from_configuration_only() -> None:
    """A manifest shaped by a result is not a preregistration."""
    parameters = set(inspect.signature(build_manifest).parameters)
    assert parameters == {"hand_built", "synthetic_seeds", "lane_threshold"}


def test_changing_the_configuration_moves_the_pinned_digest() -> None:
    """The pin is a real guard, not decoration."""
    moved = build_manifest(lane_threshold=Fraction(3, 4))
    assert moved.digest() != PREREGISTERED_MANIFEST_DIGEST


def test_every_deferral_is_a_named_manifest_row(manifest: E4Manifest) -> None:
    assert set(manifest.deferred) == set(DEFERRED)
    assert set(PARTIAL_E4_DEFERRALS) <= set(DEFERRED)
    for name, text in DEFERRED.items():
        assert text.strip(), name


def test_every_zero_tolerance_clause_carries_its_full_text(
    manifest: E4Manifest,
) -> None:
    assert set(manifest.zero_tolerance) == set(ZERO_TOLERANCE)
    for name in ZERO_TOLERANCE:
        assert manifest.zero_tolerance[name] == CLAUSES[name]


def test_the_starvation_threshold_is_zero(manifest: E4Manifest) -> None:
    """§7.2 offers no budget: a starved mandatory prerequisite is invalid."""
    assert STARVATION_THRESHOLD == 0
    assert manifest.starvation_threshold == 0


def test_every_falsification_clause_is_reported_and_not_vacuous(
    report,
) -> None:
    reported = {row.clause: row for row in report.falsifications}
    assert set(reported) == set(CLAUSES)
    for clause, row in reported.items():
        assert row.evaluated > 0, f"{clause} evaluated nothing"
        assert row.detail.strip()
        assert row.zero_tolerance == (clause in ZERO_TOLERANCE)


# --------------------------------------------------------------------------- #
# The grid is §11 E4's grid.                                                   #
# --------------------------------------------------------------------------- #
def test_every_design_named_e4_configuration_is_in_the_grid() -> None:
    built = {c.name for c in configurations()}
    assert set(DESIGN_TEXT_CONFIGURATIONS) <= built
    assert set(DESIGN_NAMED_CONFIGURATIONS) == set(DESIGN_TEXT_CONFIGURATIONS)
    named = {c.name for c in configurations() if c.design_named}
    assert named == set(DESIGN_TEXT_CONFIGURATIONS)


def test_the_added_shared_pools_exist_only_to_match_a_lane_total() -> None:
    """Additions are visible as additions, never as quotations."""
    added = {c.name for c in configurations() if not c.design_named}
    assert added == {"shared_pool_3", "shared_pool_6"}
    totals = {c.total_capacity for c in configurations()
              if c.kind == KIND_LANE}
    assert {3, 6} <= totals


def test_the_capacity_matched_partner_has_equal_total_capacity() -> None:
    by_name = {c.name: c for c in configurations()}
    pairs = capacity_matched_pairs()
    lanes = {c.name for c in configurations() if c.kind == KIND_LANE}
    assert set(pairs) == lanes
    for lane_name, shared_name in pairs.items():
        assert by_name[shared_name].kind == KIND_SHARED
        assert (by_name[shared_name].total_capacity
                == by_name[lane_name].total_capacity)


# --------------------------------------------------------------------------- #
# The compilation: lanes as configuration over named pools.                    #
# --------------------------------------------------------------------------- #
def _raw_shared_by_hand(workload, capacity: int) -> dict:
    """A shared-pool instance written WITHOUT the compiler's pool code.

    It reuses the workload's own public action and obligation
    declarations — that is the thing being held fixed — and writes the
    resource block, the demands, and the change scope by hand."""
    actions = []
    for raw in workload.actions:
        action = dict(raw)
        action["demand"] = {"pool": 1}
        actions.append(action)
    return {
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "instance_id": f"e4-{workload.name}-shared_pool_{capacity}",
        "epoch": 0,
        "cost_unit": COST_UNIT,
        "unit_mapping_version": UNIT_MAPPING_VERSION,
        "changes": [{
            "id": "c1",
            "affected_scope": sorted(
                {f"loc:{a['id']}" for a in actions} | {"loc:elsewhere"}),
        }],
        "obligations": [dict(o) for o in workload.obligations],
        "actions": actions,
        "conflict_groups": [],
        "resources": [{
            "id": "pool", "version": 1,
            "segments": [{"start": 0, "capacity": capacity}],
            "reservation": 10_000, "hard_cap": 99,
        }],
    }


def test_the_compilation_is_behavior_preserving_for_the_shared_pool() -> None:
    """Compiled and hand-written shared-pool instances are the same problem.

    Same canonical digest, and — the part that actually matters — the
    same checker verdict and the same full five-stage objective tuple for
    every candidate on the roster.

    EWD-BRR-7: the witness covers the WHOLE corpus (hand-built and
    synthetic) against EVERY shared configuration in the grid, not the
    four-workload, two-capacity sample it used to. A behaviour-preserving
    claim scoped narrower than the grid it licenses is a claim about a
    sample."""
    corpus = workloads(build_manifest())
    shared = [c for c in configurations() if c.kind == KIND_SHARED]
    compared = 0
    for workload in corpus:
        for config in shared:
            compiled = load_batch_instance(compile_instance(workload, config))
            by_hand = load_batch_instance(
                _raw_shared_by_hand(workload, config.total_capacity))
            assert compiled.digest == by_hand.digest
            for candidate, planner in CANDIDATES:
                left = _check(compiled, planner(compiled), candidate)
                right = _check(by_hand, planner(by_hand), candidate)
                assert left.verdict == right.verdict
                assert left.causes() == right.causes()
                assert (left.recomputed_objective
                        == right.recomputed_objective)
                compared += 1
    # The premise, asserted rather than hoped: this really is the whole
    # grid's shared half, over the whole corpus.
    assert len(corpus) == 6 and len(shared) == 5
    assert compared == len(corpus) * len(shared) * len(CANDIDATES)


def test_every_compiled_action_answers_for_every_declared_pool() -> None:
    """§7: an omitted demand key is a refusal, and the corpus writes zeros."""
    checked = 0
    for workload in workloads(build_manifest()):
        for config in configurations():
            raw = compile_instance(workload, config)
            pools = {r["id"] for r in raw["resources"]}
            for action in raw["actions"]:
                assert set(action["demand"]) == pools
                checked += 1
            load_batch_instance(raw)
    assert checked > 0


def test_borrowing_off_compiles_to_two_named_pools() -> None:
    workload = hand_built_workloads()[0]
    config = next(c for c in configurations()
                  if c.name == "lane_2_2_borrow_off")
    raw = compile_instance(workload, config)
    assert {r["id"] for r in raw["resources"]} == {LANE_CRITICAL, LANE_LIGHT}
    assert raw["conflict_groups"] == []
    lanes = lane_assignment(workload, config)
    for action in raw["actions"]:
        lane = lanes[action["id"]]
        other = LANE_LIGHT if lane == LANE_CRITICAL else LANE_CRITICAL
        assert action["demand"][lane] == 1
        # Written, not omitted: an absent key is never a zero.
        assert action["demand"][other] == 0


def test_borrowing_on_compiles_to_one_pool_plus_a_light_lane_cap() -> None:
    workload = hand_built_workloads()[0]
    config = next(c for c in configurations()
                  if c.name == "lane_2_2_borrow_on_asymmetric")
    raw = compile_instance(workload, config)
    assert [r["id"] for r in raw["resources"]] == ["lane_pool"]
    assert raw["resources"][0]["segments"] == [{"start": 0, "capacity": 4}]
    (group,) = raw["conflict_groups"]
    assert group["id"] == "light_lane_share"
    assert group["max_concurrent"] == 2
    lanes = lane_assignment(workload, config)
    assert set(group["members"]) == {
        a for a, lane in lanes.items() if lane == LANE_LIGHT}


def test_borrowing_on_lets_a_critical_action_use_idle_light_capacity() -> None:
    """The borrowing arm really borrows, and the checker says so.

    ``parallel-mandatory-work`` has three independent mandatory actions.
    Under (critical, light) = (1, 2) with borrowing OFF, the critical
    pool admits one at a time. With borrowing ON the same three may run
    together on the combined pool of three, because the light lane is not
    using its share. The evidence is the checked makespan."""
    workload = next(w for w in hand_built_workloads()
                    if w.name == "parallel-mandatory-work")
    off = next(c for c in configurations() if c.name == "lane_1_2_borrow_off")
    on = next(c for c in configurations()
              if c.name == "lane_1_2_borrow_on_asymmetric")
    results = {}
    for config in (off, on):
        instance = load_batch_instance(compile_instance(workload, config))
        plan = replay._plan_candidate(instance, "fifo")
        result = _check(instance, plan)
        assert result.accepted, result.causes()
        assert result.recomputed_objective is not None
        results[config.borrowing] = result.recomputed_objective.makespan
    assert results[BORROWING_ON_ASYMMETRIC] < results[BORROWING_OFF], results


def test_borrowing_on_never_lets_the_light_lane_exceed_its_share() -> None:
    """Borrowing is asymmetric, and the CHECKER enforces the asymmetry.

    ``light-heavy-one-mandatory`` has four light actions. Under
    (critical, light) = (1, 1) with borrowing ON the pool holds two, but
    the light-lane group admits one, so a plan that runs two light
    actions at once is refused for the group — not for capacity."""
    workload = next(w for w in hand_built_workloads()
                    if w.name == "light-heavy-one-mandatory")
    config = next(c for c in configurations()
                  if c.name == "lane_1_1_borrow_on_asymmetric")
    instance = load_batch_instance(compile_instance(workload, config))
    plan = replay._plan_candidate(instance, "fifo")
    base = _check(instance, plan)
    assert base.accepted, base.causes()
    assert base.recomputed_objective is not None

    lanes = lane_assignment(workload, config)
    light = sorted(a for a, lane in lanes.items() if lane == LANE_LIGHT)
    by_id = {s.action_id: s for s in plan.selected}
    first, second = light[0], light[1]
    overlapped = replay._shifted_plan(
        plan, base.recomputed_objective, second, by_id[first].start)
    result = _check(instance, overlapped)
    assert not result.accepted
    assert CAUSE_CONFLICT_GROUP_VIOLATION in result.causes()


def test_symmetric_borrowing_is_a_named_deferral_with_the_schema_gap(
    manifest: E4Manifest,
) -> None:
    """The unexpressible arm is a row, never a silent approximation."""
    text = DEFERRED["symmetric_capacity_borrowing"]
    assert "demand(a, r)" in text
    assert "CONSTANT" in text
    assert "symmetric_capacity_borrowing" in PARTIAL_E4_DEFERRALS
    assert manifest.deferred["symmetric_capacity_borrowing"] == text
    # The expressible arm is not deferred: it RAN.
    assert any(c.borrowing == BORROWING_ON_ASYMMETRIC
               for c in configurations())
    assert any(c.borrowing == BORROWING_OFF for c in configurations())


def test_preemption_is_a_named_deferral_row() -> None:
    assert "non-preemptive" in DEFERRED["preemption"]
    assert "chunk" in DEFERRED["preemption"]


def test_the_unit_demand_bound_on_the_lane_compilation_is_a_named_row(
) -> None:
    """Any bound the harness imposes is a row, never a silent cap."""
    assert "unit_demand_lane_compilation" in DEFERRED
    assert "unit_demand_lane_compilation" in PARTIAL_E4_DEFERRALS
    for config in configurations():
        if config.kind != KIND_LANE:
            continue
        for workload in hand_built_workloads():
            raw = compile_instance(workload, config)
            for action in raw["actions"]:
                assert set(action["demand"].values()) <= {0, 1}


# --------------------------------------------------------------------------- #
# Every capacity event: the interior breakpoint.                               #
# --------------------------------------------------------------------------- #
def test_an_interior_capacity_breakpoint_decides_feasibility(report) -> None:
    """A capacity drop that is nobody's start and nobody's end decides.

    The control is what makes it attributable: the same plan under a flat
    capacity is ACCEPTED, so the refusal is the breakpoint's doing."""
    witness = capacity_event_witness()
    assert witness.refused
    assert CAUSE_CAPACITY_EXCEEDED in witness.causes
    assert witness.control_accepted
    assert witness.control_causes == ()
    # Interior: the breakpoint lies strictly inside a running action's
    # window, so a start-and-end-only scan could not have seen it.
    assert witness.interior_to == ("w_a", "w_b")
    assert report.capacity_event.refused
    assert report.capacity_event.interior_to == witness.interior_to


def test_the_report_is_invalid_if_the_capacity_witness_stops_firing(
    report,
) -> None:
    """Vacuity guard: a witness that stopped biting invalidates the run."""
    neutered = replace(report, capacity_event=replace(
        report.capacity_event, refused=False, causes=()))
    assert neutered.verdict == "invalid:capacity_event_witness_did_not_fire"


# --------------------------------------------------------------------------- #
# Lane validity: §7.2's mandatory-closure requirement.                         #
# --------------------------------------------------------------------------- #
def test_the_reviewed_lane_rule_admits_the_whole_mandatory_closure() -> None:
    checked = 0
    for workload in workloads(build_manifest()):
        closure = mandatory_closure(workload)
        assert closure, workload.name
        for config in configurations():
            if config.lane_rule != LANE_RULE_MANDATORY_CLOSURE:
                continue
            lanes = lane_assignment(workload, config)
            assert all(lanes[a] == LANE_CRITICAL for a in closure)
            assert lane_config_validity(workload, config) == (
                LANE_CONFIG_VALID, ())
            checked += 1
    assert checked > 0


def test_the_mandatory_closure_spans_more_than_one_action_class() -> None:
    """The premise the invalidity probe needs, asserted rather than hoped.

    A closure confined to one action class could not be split by a
    class-based rule, and the falsifier below would be firing on a
    configuration nothing could have got wrong."""
    workload = next(w for w in hand_built_workloads()
                    if w.name == "gated-mandatory-closure")
    by_id = {str(a["id"]): a for a in workload.actions}
    classes = {by_id[a]["action_class"] for a in mandatory_closure(workload)}
    assert len(classes) >= 2, classes


def _class_based_lane_config() -> ResourceConfig:
    return ResourceConfig(
        name="lane_2_2_action_class_diagnostic",
        kind=KIND_LANE,
        pools={LANE_CRITICAL: ({"start": 0, "capacity": 2},),
               LANE_LIGHT: ({"start": 0, "capacity": 2},)},
        lane_rule=LANE_RULE_ACTION_CLASS,
        borrowing=BORROWING_OFF,
        shares=(2, 2),
        total_capacity=4,
        design_named=False,
        exercises="diagnostic: a class-based rule that splits the closure",
    )


def test_a_class_based_lane_rule_is_graded_invalid() -> None:
    """§7.2: a configuration that starves a mandatory prerequisite is invalid."""
    config = _class_based_lane_config()
    workload = next(w for w in hand_built_workloads()
                    if w.name == "gated-mandatory-closure")
    verdict, excluded = lane_config_validity(workload, config)
    assert verdict == LANE_CONFIG_EXCLUDES_MANDATORY_CLOSURE
    assert set(excluded) == {"g_check", "g_gate"}
    assert set(excluded) <= set(mandatory_closure(workload))


def test_the_invalidity_falsifier_fires_on_a_starving_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The zero-tolerance clause is reachable, and it refutes the run."""
    extended = configurations() + (_class_based_lane_config(),)
    monkeypatch.setattr(replay, "configurations", lambda: extended)
    manifest = build_manifest(
        hand_built=("gated-mandatory-closure",), synthetic_seeds=())
    result = run_report(manifest)
    assert result.verdict == f"refuted:{CLAUSE_LANE_EXCLUDES_MANDATORY_CLOSURE}"
    row = next(r for r in result.falsifications
               if r.clause == CLAUSE_LANE_EXCLUDES_MANDATORY_CLOSURE)
    assert row.fired and row.zero_tolerance
    assert "g_check" in row.detail
    # Only the diagnostic configuration is implicated.
    implicated = {o.config for o in result.outcomes
                  if o.lane_validity != LANE_CONFIG_VALID}
    assert implicated == {"lane_2_2_action_class_diagnostic"}


# --------------------------------------------------------------------------- #
# Starvation: fires on a starving plan, quiet on an honest delay.              #
# --------------------------------------------------------------------------- #
def _two_action_instance(capacity: int, light_duration: int):
    raw = {
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "instance_id": "e4-starvation-probe",
        "epoch": 0,
        "cost_unit": COST_UNIT,
        "unit_mapping_version": UNIT_MAPPING_VERSION,
        "changes": [{"id": "c1", "affected_scope": ["loc:l_a", "loc:m_a"]}],
        "obligations": [
            replay._obligation("o_must", PRIORITY_MUST, "probe-must",
                               obligation_class="mandatory"),
            replay._obligation("o_should", PRIORITY_SHOULD, "probe-should"),
        ],
        "actions": [],
        "conflict_groups": [],
        "resources": [{
            "id": "pool", "version": 1,
            "segments": [{"start": 0, "capacity": capacity}],
            "reservation": 10_000, "hard_cap": 99,
        }],
    }
    light = replay._work("l_a", covers=["o_should"],
                         duration=light_duration, execution_cost=1)
    mandatory = replay._work("m_a", action_class="repair", covers=["o_must"],
                             duration=1, execution_cost=1)
    for action in (light, mandatory):
        action["demand"] = {"pool": 1}
    raw["actions"] = [light, mandatory]
    return load_batch_instance(raw)


def test_the_starvation_probe_fires_when_the_checker_had_room() -> None:
    """A mandatory action pushed late while the pool idles IS starved."""
    instance = _two_action_instance(capacity=2, light_duration=1)
    plan = replay._plan_candidate(instance, "fifo")
    base = _check(instance, plan)
    assert base.accepted and base.recomputed_objective is not None
    delayed = replay._shifted_plan(plan, base.recomputed_objective, "m_a", 3)
    delayed_result = _check(instance, delayed)
    assert delayed_result.accepted, delayed_result.causes()
    assert delayed_result.recomputed_objective is not None

    starved, probed = single_move_starved_actions(
        instance, delayed, delayed_result.recomputed_objective,
        ("m_a",), "fifo")
    assert probed == 1
    assert starved == ("m_a",)


def test_the_starvation_probe_is_quiet_when_a_delay_is_not_starvation(
) -> None:
    """A delay the capacity curve forced is a delay, not starvation.

    The premise is asserted, not assumed: the mandatory action really did
    WAIT (a nonzero delay, and a probe that ran), and the probe still
    reports nothing, because no earlier start is one the checker accepts.
    Without the probe count this test would pass on a probe that never
    looked."""
    instance = _two_action_instance(capacity=1, light_duration=2)
    plan = replay._plan_candidate(instance, "fifo")
    result = _check(instance, plan)
    assert result.accepted and result.recomputed_objective is not None
    by_id = {s.action_id: s for s in plan.selected}
    assert by_id["m_a"].start == 2, plan.selected

    starved, probed = single_move_starved_actions(
        instance, plan, result.recomputed_objective, ("m_a",), "fifo")
    assert probed == 1
    assert starved == ()


def test_the_starvation_clause_evaluated_real_probes(report) -> None:
    row = next(r for r in report.falsifications
               if r.clause == CLAUSE_MANDATORY_PREREQUISITE_STARVED)
    assert row.evaluated == sum(o.single_move_probes for o in report.outcomes)
    assert row.evaluated > 0
    assert not row.fired
    assert all(o.single_move_starved == () for o in report.outcomes)


def test_mandatory_delay_is_reported_even_where_nothing_is_starved(
    report,
) -> None:
    """Delay and starvation are different numbers, and both are reported."""
    delayed = [o for o in report.outcomes
               if (o.mandatory_delay_total or 0) > 0]
    assert delayed, "no mandatory work waited anywhere in the grid"
    for outcome in delayed:
        assert outcome.mandatory_delay_max is not None
        assert outcome.mandatory_delay_max <= outcome.queue_delay_max


# --------------------------------------------------------------------------- #
# The capacity-matched comparison.                                             #
# --------------------------------------------------------------------------- #
def test_the_comparison_is_computed_from_checked_tuples(report) -> None:
    """Re-derive one configuration's `better` count from the outcomes."""
    pairs = capacity_matched_pairs()
    by_key = {(o.config, o.workload, o.candidate): o for o in report.outcomes}
    for row in report.comparison.per_config:
        shared = pairs[row.config]
        better = 0
        comparable = 0
        for (config, workload, candidate), outcome in by_key.items():
            if config != row.config:
                continue
            base = by_key.get((shared, workload, candidate))
            if base is None or base.objective is None:
                continue
            if (outcome.status != STATUS_CHECKED_FEASIBLE
                    or base.status != STATUS_CHECKED_FEASIBLE
                    or outcome.objective is None):
                continue
            comparable += 1
            if (objective_sort_key(outcome.objective)[:4]
                    < objective_sort_key(base.objective)[:4]):
                better += 1
        assert row.better == better, row.config
        assert row.beats_matched_pool == f"{better}/{comparable}"


def test_the_comparison_excludes_only_the_tie_break_stage(
    manifest: E4Manifest,
) -> None:
    """The one-stage exclusion is preregistered and is one stage wide."""
    assert "stages 1-4" in manifest.primary_metric
    assert "Stage 5 is EXCLUDED" in manifest.primary_metric
    sample = ObjectiveTuple(should_units=1, may_units=1, total_cost=1,
                            makespan=1, tie_break="t")
    assert len(objective_sort_key(sample)) == COMPARISON_STAGES + 1
    assert COMPARISON_STAGES == 4


def test_the_comparator_is_the_trusted_bases_ordering_truncated() -> None:
    """EWD-BRR-1: the §8 ordering is TAKEN, never re-derived here.

    A local re-derivation is a fork, and this corpus never lets stages
    1-3 decide — so three quarters of a forked comparator could be
    deleted with no test and no report byte noticing. The comparator is
    now a truncation of the trusted base's own key, and this asserts the
    identity directly."""
    for should, may, cost, makespan in (
        (0, 0, 0, 0), (3, 1, 7, 4), (1, 3, 4, 7), (9, 9, 9, 9),
    ):
        objective = ObjectiveTuple(
            should_units=should, may_units=may, total_cost=cost,
            makespan=makespan, tie_break="whatever")
        assert (replay._stage_key(objective)
                == objective_sort_key(objective)[:COMPARISON_STAGES])


def _objective(should: int, may: int, cost: int, makespan: int,
               tie: str = "tb") -> ObjectiveTuple:
    return ObjectiveTuple(should_units=should, may_units=may,
                          total_cost=cost, makespan=makespan, tie_break=tie)


def _outcome(config: str, workload: str, candidate: str,
             objective: ObjectiveTuple | None = None,
             *, status: str = STATUS_CHECKED_FEASIBLE,
             mandatory_delay_total: int = 0) -> replay.ReplayOutcome:
    """One hand-built comparable row. Only the compared fields vary."""
    return replay.ReplayOutcome(
        workload=workload, workload_digest="w" * 64, config=config,
        config_digest="c" * 64, candidate=candidate,
        roster_version=replay.REPLAY_ROSTER_VERSION,
        manifest_digest="m" * 64, status=status, causes=(),
        qualified=status == STATUS_CHECKED_FEASIBLE, objective=objective,
        makespan=(None if objective is None else objective.makespan),
        utilization={}, queue_delay_total=0, queue_delay_max=0,
        mandatory_delay_total=mandatory_delay_total, mandatory_delay_max=0,
        single_move_starved=(), single_move_probes=0,
        inversion_starved=(), inversion_probes=0,
        capacity_violation_causes=(), not_established=(),
        lane_validity=LANE_CONFIG_VALID, lane_excluded=(),
        plan_digest="p" * 64, certificate_digest="x" * 64)


#: One directed pair per §8 stage: the lane arm and the shared arm differ
#: in EXACTLY that stage, and the lane arm is the better one. Stage 4 is
#: included so the pairs cover the whole truncated key.
_STAGE_DIRECTED_PAIRS = (
    ("stage_1_should_units", _objective(3, 1, 5, 5), _objective(2, 1, 5, 5)),
    ("stage_2_may_units", _objective(2, 4, 5, 5), _objective(2, 1, 5, 5)),
    ("stage_3_total_cost", _objective(2, 1, 3, 5), _objective(2, 1, 5, 5)),
    ("stage_4_makespan", _objective(2, 1, 5, 2), _objective(2, 1, 5, 5)),
)


def _directed_comparison() -> replay.ComparisonRow:
    """Run the REAL comparison over the four directed pairs."""
    lane_name = "lane_1_1_borrow_off"
    shared_name = capacity_matched_pairs()[lane_name]
    outcomes = []
    for index, (stage, lane_objective, shared_objective) in enumerate(
            _STAGE_DIRECTED_PAIRS):
        workload = f"directed-{stage}"
        candidate = CANDIDATES[index][0]
        outcomes.append(_outcome(lane_name, workload, candidate,
                                 lane_objective))
        outcomes.append(_outcome(shared_name, workload, candidate,
                                 shared_objective))
    return replay._comparison_row(
        outcomes, Fraction(1, 2), (), minimum_comparable=1)


def test_each_of_the_four_stages_decides_a_directed_pair() -> None:
    """EWD-BRR-1: every stage is exercised independently of the corpus.

    The corpus never makes stages 1-3 differ, which is exactly why the
    comparator needs a test that does not depend on it. Four hand-built
    pairs differ in ONE stage each; the lane arm is better in all four,
    and the reported deciding stage names the stage that decided."""
    comparison = _directed_comparison()
    row = next(r for r in comparison.per_config
               if r.config == "lane_1_1_borrow_off")
    assert row.comparable == len(_STAGE_DIRECTED_PAIRS)
    assert row.better == len(_STAGE_DIRECTED_PAIRS), row.deciding_stages
    assert row.tied == row.worse == 0
    assert dict(row.deciding_stages) == {
        stage: 1 for stage, _, _ in _STAGE_DIRECTED_PAIRS}


def test_a_makespan_only_comparator_would_fail_the_directed_pairs() -> None:
    """The neutering EWD-BRR-1 slipped past the suite now FAILS.

    The reviewer replaced the comparator's body with
    ``(0, 0, 0, makespan)`` — discarding should units, may units, and
    cost — and the whole report stayed byte-identical. This asserts, on
    the four directed pairs, that the neutered key and the §8 key
    DISAGREE, so a comparator returning the neutered value could not
    produce the counts the test above pins."""
    neutered_better = 0
    for _stage, lane_objective, shared_objective in _STAGE_DIRECTED_PAIRS:
        if ((0, 0, 0, lane_objective.makespan)
                < (0, 0, 0, shared_objective.makespan)):
            neutered_better += 1
        # And the real key gets it right on every one of them.
        assert (objective_sort_key(lane_objective)[:COMPARISON_STAGES]
                < objective_sort_key(shared_objective)[:COMPARISON_STAGES])
    # Three of the four pairs are invisible to a makespan-only key.
    assert neutered_better == 1
    row = next(r for r in _directed_comparison().per_config
               if r.config == "lane_1_1_borrow_off")
    assert neutered_better != row.better


def test_arms_that_disagree_on_a_binding_field_are_not_compared(
    report,
) -> None:
    """SIT-1: two arms that are not the same experiment are not subtracted."""
    assert COMPARISON_BINDING_FIELDS == (
        "candidate", "manifest_digest", "roster_version", "workload_digest")
    lane = next(c for c in configurations() if c.kind == KIND_LANE)
    poisoned = tuple(
        replace(o, workload_digest="0" * 64) if o.config == lane.name else o
        for o in report.outcomes
    )
    row = next(r for r in replay._comparison_row(
        poisoned, Fraction(1, 2)).per_config if r.config == lane.name)
    assert row.binding_mismatch > 0
    assert row.better == row.tied == row.worse == 0
    # A mismatch is reported as a mismatch, never absorbed into a tie.
    clean = next(r for r in report.comparison.per_config
                 if r.config == lane.name)
    assert clean.binding_mismatch == 0
    assert clean.better + clean.tied + clean.worse > 0


def test_the_lanes_conclusion_is_derived_from_the_comparison(report) -> None:
    comparison = report.comparison
    expected = (CONCLUSION_HARDCODING_SUPPORTED if comparison.met
                else CONCLUSION_NOT_HARDCODED)
    assert comparison.conclusion == expected
    assert comparison.established
    row = next(r for r in report.falsifications
               if r.clause == CLAUSE_CAPACITY_MATCHED_COMPARISON)
    assert row.fired == (not comparison.met)
    assert comparison.conclusion in row.detail
    assert report.to_canonical_dict()["lanes_conclusion"] == (
        comparison.conclusion)


def test_the_headline_names_the_configurations_that_never_won(report) -> None:
    """A pool headline may not read as a claim about every member."""
    headline = report.comparison.headline
    for row in report.comparison.per_config:
        if row.better > 0:
            assert row.config in headline
    assert "INDISTINGUISHABLE" in headline
    assert "stages that decided nothing here" in headline


# --------------------------------------------------------------------------- #
# Verdict, determinism, and the checker's authority.                           #
# --------------------------------------------------------------------------- #
def test_the_report_is_byte_equal_across_runs(manifest: E4Manifest) -> None:
    first = run_report(manifest)
    second = run_report(build_manifest())
    assert first.canonical_json() == second.canonical_json()
    assert first.digest() == second.digest()


def test_a_partial_e4_run_never_reports_a_bare_pass(report) -> None:
    """The 'a deferral is a row' standard applies to the verdict too."""
    assert report.verdict != VERDICT_PASS
    cleared = replace(
        report,
        falsifications=tuple(replace(row, fired=False)
                             for row in report.falsifications))
    assert cleared.verdict.startswith(f"{VERDICT_PASS_PARTIAL_E4}:")
    for name in PARTIAL_E4_DEFERRALS:
        assert name in cleared.verdict
    full = replace(cleared, deferred={})
    assert full.verdict == VERDICT_PASS


def test_the_verdict_is_the_lane_claim_rejection_not_a_refutation(
    report,
) -> None:
    """A missed performance threshold rejects the LANE claim, not the run.

    §11 E4's own sentence: in that case lanes remain an experiment
    configuration and are not hardcoded."""
    fired = set(report.fired())
    assert not (fired & set(ZERO_TOLERANCE)), fired
    if CLAUSE_CAPACITY_MATCHED_COMPARISON in fired:
        assert report.verdict == (
            f"lane_claim_rejected:{CLAUSE_CAPACITY_MATCHED_COMPARISON}")
        assert report.comparison.conclusion == CONCLUSION_NOT_HARDCODED


def test_every_reported_qualification_is_the_checkers_own_verdict(
    report,
) -> None:
    """The harness never softens, overrides, or re-derives a refusal."""
    for outcome in report.outcomes:
        assert outcome.qualified == (
            outcome.status == STATUS_CHECKED_FEASIBLE and not outcome.causes)
        if not outcome.qualified:
            assert outcome.single_move_starved == ()
            assert outcome.inversion_starved == ()
    assert any(o.qualified for o in report.outcomes)


def test_the_module_reads_no_clock_and_draws_no_random_value() -> None:
    source = inspect.getsource(replay)
    for banned in ("import random", "time.time", "perf_counter",
                   "monotonic", "datetime"):
        assert banned not in source, banned
    assert "SynthPrf" in source


def test_the_report_carries_no_float(report) -> None:
    """A float in digested content is a digest that moves between machines."""
    def walk(value) -> None:
        assert not isinstance(value, float), value
        if isinstance(value, dict):
            for item in value.values():
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(json.loads(report.canonical_json()))


def test_the_grid_is_large_enough_to_have_measured_something(report) -> None:
    """A clear run over an empty grid is not a clear run."""
    assert len(report.outcomes) == (
        len(workloads(build_manifest())) * len(configurations())
        * len(CANDIDATES))
    assert len(report.workloads) >= 5
    assert all(c.lane_rule in (LANE_RULE_NONE, LANE_RULE_MANDATORY_CLOSURE)
               for c in configurations())


# --------------------------------------------------------------------------- #
# EWD-BRR-2: a refusing row is LOUD, and 0/0 concludes nothing.                #
# --------------------------------------------------------------------------- #
def _hard_cap_zero(poisoned: str):
    """The reviewer's injection: one workload's pools get hard_cap 0.

    No source is mutated. ``compile_instance`` is wrapped so that every
    instance built for the named workload declares a hard cap the plans
    cannot satisfy, and the CHECKER refuses those rows — exactly the
    "one sixth of the experiment vanishes" shape the review executed."""
    original = replay.compile_instance

    def wrapped(workload, config):
        raw = original(workload, config)
        if workload.name == poisoned or poisoned == "*":
            for resource in raw["resources"]:
                resource["hard_cap"] = 0
        return raw

    return wrapped


def test_a_refusing_row_is_counted_loudly_and_named_by_the_verdict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """EWD-BRR-2: a shrunken experiment may not hide in a denominator.

    Before: one sixth of the grid refused, every denominator dropped 30
    to 25, the verdict and headline were byte-identical, and the only
    trace was `not_comparable` inside `per_config`."""
    monkeypatch.setattr(
        replay, "compile_instance",
        _hard_cap_zero("mandatory-chain-under-contention"))
    result = run_report(build_manifest())

    census = result.refusal_census()
    assert census["total"] > 0
    assert census["workload:mandatory-chain-under-contention"] == census[
        "total"]
    # The verdict NAMES the refusals, so a reader cannot miss them.
    assert f":{VERDICT_REFUSALS_SUFFIX}:{census['total']}" in result.verdict
    assert result.to_canonical_dict()["refusal_census"]["total"] == census[
        "total"]
    # And every comparison row carries its own refused count beside the
    # denominator that shrank.
    for row in result.comparison.per_config:
        assert row.refused > 0
        assert row.comparable == row.better + row.tied + row.worse
        assert row.beats_matched_pool == f"{row.better}/{row.comparable}"
    assert "REFUSAL" in result.comparison.headline


def test_a_clean_run_carries_no_refusal_suffix(report) -> None:
    """The counterpart: the suffix is a signal, not decoration."""
    assert report.refusal_census()["total"] == 0
    assert VERDICT_REFUSALS_SUFFIX not in report.verdict
    assert all(row.refused == 0 for row in report.comparison.per_config)


def test_a_comparison_over_zero_pairs_concludes_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """EWD-BRR-2: `met` made 'measured nothing' and 'measured and lost'
    the same value, and the report published the negative lane sentence
    over a 0/0 denominator beneath an `invalid:` verdict."""
    monkeypatch.setattr(replay, "compile_instance", _hard_cap_zero("*"))
    result = run_report(build_manifest())

    assert all(o.status != STATUS_CHECKED_FEASIBLE for o in result.outcomes)
    assert result.comparison.best_comparable == 0
    assert not result.comparison.established
    assert not result.comparison.met
    conclusion = result.to_canonical_dict()["lanes_conclusion"]
    assert conclusion == CONCLUSION_NOT_ESTABLISHED
    assert conclusion != CONCLUSION_NOT_HARDCODED
    assert result.verdict.startswith("invalid:no_checked_feasible_candidate")
    assert VERDICT_REFUSALS_SUFFIX in result.verdict
    # The clause that could not be evaluated did not FAIL.
    row = next(r for r in result.falsifications
               if r.clause == CLAUSE_CAPACITY_MATCHED_COMPARISON)
    assert not row.fired
    assert "NOT EVALUATED" in row.detail


def test_a_denominator_below_the_preregistered_minimum_refuses_to_conclude(
) -> None:
    """The floor is a preregistered number, and it is enforced.

    A single hand-built workload leaves five comparable pairs per
    configuration — a real measurement, and still too thin a base for a
    sentence about the corpus."""
    manifest = build_manifest(
        hand_built=("mandatory-chain-under-contention",), synthetic_seeds=())
    assert manifest.minimum_comparable_pairs == MINIMUM_COMPARABLE_PAIRS
    result = run_report(manifest)
    assert 0 < result.comparison.best_comparable < MINIMUM_COMPARABLE_PAIRS
    assert not result.comparison.established
    assert result.comparison.conclusion == CONCLUSION_NOT_ESTABLISHED
    assert result.verdict == "invalid:comparison_denominator_below_minimum"
    assert "NOT ESTABLISHED" in result.comparison.headline


def test_an_invalid_verdict_publishes_no_lane_conclusion(report) -> None:
    """A substantive claim beneath `invalid:` is a claim nobody earned."""
    neutered = replace(report, capacity_event=replace(
        report.capacity_event, refused=False, causes=()))
    assert neutered.verdict.startswith("invalid:")
    assert neutered.comparison.established
    assert neutered.lanes_conclusion() == CONCLUSION_NOT_ESTABLISHED_INVALID
    assert neutered.to_canonical_dict()["lanes_conclusion"] == (
        CONCLUSION_NOT_ESTABLISHED_INVALID)


# --------------------------------------------------------------------------- #
# EWD-BRR-3: the priority-inversion probe §7.2 actually implies.               #
# --------------------------------------------------------------------------- #
def test_the_inversion_probe_counts_the_reviewers_worst_row(report) -> None:
    """The row the old probe exonerated is now COUNTED.

    ``mandatory-chain-under-contention`` under ``shared_pool_1`` with
    ``tier_earliest_finish``: the mandatory ``m_probe`` is ready at 0 and
    starts at 6, behind two `should` actions and one `may`. The
    single-move probe says nothing — correctly, the pool was never idle —
    and the inversion probe says the lower-priority work should have
    waited instead."""
    outcome = next(
        o for o in report.outcomes
        if o.workload == "mandatory-chain-under-contention"
        and o.config == "shared_pool_1"
        and o.candidate == "tier_earliest_finish")
    assert outcome.status == STATUS_CHECKED_FEASIBLE
    assert outcome.inversion_probes > 0
    assert "m_probe" in outcome.inversion_starved
    # The two metrics count different things, and neither absorbs the
    # other: the single-move probe is silent on this very row.
    assert outcome.single_move_starved == ()


def test_the_inversion_metric_is_reported_and_is_not_vacuous(report) -> None:
    """A metric that cannot fire is not a metric. This one fires."""
    row = next(r for r in report.falsifications
               if r.clause == CLAUSE_MANDATORY_PREREQUISITE_INVERSION)
    assert row.evaluated == sum(o.inversion_probes for o in report.outcomes)
    assert row.evaluated > 0
    assert row.zero_tolerance
    total = sum(len(o.inversion_starved) for o in report.outcomes)
    assert total > 0, "the inversion metric found nothing anywhere"
    assert str(total) in row.detail or "REPORTED" in row.detail
    # Every row's canonical dict carries the metric, not just the clause.
    assert "inversion_starved" in report.outcomes[0].to_canonical_dict()


def test_the_inversion_falsifier_is_scoped_to_lane_configurations(
    report,
) -> None:
    """§7.2 conditions the promise, so the falsifier is conditioned too.

    "If a critical lane is configured, it must admit the full
    prerequisite closure of mandatory actions." A generic shared pool
    configures no lane and makes no lane promise, so an inversion under
    one is a REPORTED measurement, not an invalid configuration. Under a
    lane configuration the promise is live and the threshold is zero.

    On this corpus that scoping is what makes the metric discriminate:
    every inversion found is under a shared or heterogeneous pool, and
    the lane configurations — whose light lane can never hold more than
    its own share — produce none."""
    lanes = {c.name for c in configurations() if c.kind == KIND_LANE}
    under_lane = [o for o in report.outcomes
                  if o.config in lanes and o.inversion_starved]
    under_pool = [o for o in report.outcomes
                  if o.config not in lanes and o.inversion_starved]
    assert under_pool, "the scoping would be untested with no pool rows"
    row = next(r for r in report.falsifications
               if r.clause == CLAUSE_MANDATORY_PREREQUISITE_INVERSION)
    assert row.fired == bool(under_lane)
    # The unscoped rows are named in the detail, never dropped.
    assert "REPORTED, NOT VALIDITY-BEARING" in row.detail
    assert str(sum(len(o.inversion_starved) for o in under_pool)) in row.detail
    # The scoping decision is documented against §7.2 where it is made.
    clause_text = CLAUSES[CLAUSE_MANDATORY_PREREQUISITE_INVERSION]
    assert "§7.2" in clause_text
    assert "IF a critical lane is configured" in clause_text
    assert inversion_starved_actions.__doc__ is not None
    assert "§7.2" in replay.run_report.__doc__ or "§7.2" in clause_text


def test_the_inversion_falsifier_fires_under_a_lane_configuration() -> None:
    """The scoped arm is REACHABLE, not merely quiet.

    A hand-built lane instance in which the light lane holds the whole
    pool while a mandatory action waits: the clause must fire there, or
    the zero it reports on the grid would be a zero nobody could move."""
    lanes = {c.name for c in configurations() if c.kind == KIND_LANE}
    lane_name = sorted(lanes)[0]
    outcomes = [
        _outcome(lane_name, "w", "fifo", _objective(1, 1, 1, 4)),
        _outcome(capacity_matched_pairs()[lane_name], "w", "fifo",
                 _objective(1, 1, 1, 4)),
    ]
    poisoned = (replace(outcomes[0], inversion_starved=("m_probe",),
                        inversion_probes=1), outcomes[1])
    lane_rows = [o for o in poisoned
                 if o.config in lanes and o.inversion_starved]
    assert lane_rows, "premise: the poisoned row is under a LANE config"

    # The same derivation run_report performs, over the poisoned rows.
    fired = bool(lane_rows)
    assert fired
    row = replay.FalsificationRow(
        clause=CLAUSE_MANDATORY_PREREQUISITE_INVERSION, fired=fired,
        zero_tolerance=True, evaluated=1, detail="lane inversion")
    report = replace(
        run_report(build_manifest()),
        falsifications=(row,), outcomes=poisoned)
    assert report.verdict == (
        f"refuted:{CLAUSE_MANDATORY_PREREQUISITE_INVERSION}")


def test_the_single_move_probe_is_named_and_bounded_by_a_deferral(
    manifest: E4Manifest,
) -> None:
    """EWD-BRR-3(a): the locality is a ROW, and the name says it."""
    assert "starvation_probe_is_single_move_only" in DEFERRED
    assert "inversion_probe_displacement_rule" in DEFERRED
    assert manifest.deferred["starvation_probe_is_single_move_only"] == (
        DEFERRED["starvation_probe_is_single_move_only"])
    assert "single_move_starvation" in manifest.metric_definitions
    assert "inversion_starvation" in manifest.metric_definitions
    assert "starvation" not in manifest.metric_definitions
    assert CLAUSE_MANDATORY_PREREQUISITE_STARVED.endswith(
        "single_move_starved")
    assert CLAUSE_MANDATORY_PREREQUISITE_INVERSION in ZERO_TOLERANCE
    assert CLAUSE_MANDATORY_PREREQUISITE_INVERSION in CLAUSES


def test_the_inversion_probe_is_quiet_when_no_lower_priority_work_ran(
) -> None:
    """A capacity-forced delay with NO lower-priority occupancy is not an
    inversion, and the probe does not even count a probe for it."""
    instance = _two_action_instance(capacity=1, light_duration=2)
    plan = replay._plan_candidate(instance, "fifo")
    result = _check(instance, plan)
    assert result.accepted and result.recomputed_objective is not None
    # Restrict the mandatory set so l_a is not "lower priority": with the
    # closure covering both, there is no strictly-lower-priority work.
    inverted, probed = inversion_starved_actions(
        instance, plan, result.recomputed_objective, ("m_a", "l_a"), "fifo")
    assert probed == 0
    assert inverted == ()


def test_the_inversion_probe_fires_when_lower_priority_work_held_the_pool(
) -> None:
    """The directed positive: capacity 1, a `should` action running, and
    the mandatory action waiting behind it."""
    instance = _two_action_instance(capacity=1, light_duration=2)
    plan = replay._plan_candidate(instance, "fifo")
    result = _check(instance, plan)
    assert result.accepted and result.recomputed_objective is not None
    by_id = {s.action_id: s for s in plan.selected}
    assert by_id["l_a"].start == 0 and by_id["m_a"].start == 2

    inverted, probed = inversion_starved_actions(
        instance, plan, result.recomputed_objective, ("m_a",), "fifo")
    assert probed == 1
    assert inverted == ("m_a",)
    # And the single-move probe stays silent on the same plan, which is
    # the whole reason the second metric exists.
    starved, single_probed = single_move_starved_actions(
        instance, plan, result.recomputed_objective, ("m_a",), "fifo")
    assert single_probed == 1
    assert starved == ()


# --------------------------------------------------------------------------- #
# EWD-BRR-4: an unbindable cap is arithmetic, not a measurement.               #
# --------------------------------------------------------------------------- #
def test_an_unbindable_borrowing_cap_is_reported_as_structural(
    report,
) -> None:
    """`lane_2_4_borrow_on_asymmetric` cannot be distinguished from
    `shared_pool_6` by ANY workload in this corpus: the widest light lane
    is 4 actions and the light share is 4, so the cap never binds. That
    is a theorem of the arithmetic, and reporting it as a measured
    indistinguishability presents a silence as evidence."""
    rows = {row["config"]: row
            for row in report.comparison.indistinguishable_from_shared}
    assert "lane_2_4_borrow_on_asymmetric" in rows
    row = rows["lane_2_4_borrow_on_asymmetric"]
    assert row["reason"] == "structurally_unbindable"
    assert "max_concurrent=4" in row["detail"]
    assert "can never bind" in row["detail"]
    assert "STRUCTURALLY" in report.comparison.headline
    # The premise, computed from the corpus rather than asserted: no
    # workload has more than four light actions under that split.
    config = next(c for c in configurations()
                  if c.name == "lane_2_4_borrow_on_asymmetric")
    widths = [
        sum(1 for lane in lane_assignment(w, config).values()
            if lane == LANE_LIGHT)
        for w in workloads(build_manifest())
    ]
    assert max(widths) <= 4


def test_a_binding_borrowing_cap_is_not_called_structural() -> None:
    """The counterpart: the narrower splits CAN bind, so they are never
    labelled unbindable, and the attack that the whole borrowing-ON arm
    is vacuous fails."""
    corpus = workloads(build_manifest())
    for name in ("lane_1_1_borrow_on_asymmetric",
                 "lane_1_2_borrow_on_asymmetric",
                 "lane_2_2_borrow_on_asymmetric"):
        config = next(c for c in configurations() if c.name == name)
        assert replay._light_lane_cap_bindability(config, corpus) is None
    # A borrowing-OFF split has no light-lane cap to be unbindable.
    off = next(c for c in configurations() if c.name == "lane_2_2_borrow_off")
    assert replay._light_lane_cap_bindability(off, corpus) is None


# --------------------------------------------------------------------------- #
# EWD-BRR-5: ONE local digest dialect, with the base's refusals.               #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("value", [
    1.5, float("nan"), float("inf"), Fraction(1, 2), {"a": 0.1},
    ["ok", 2.0], b"bytes", object(),
])
def test_the_local_digest_refuses_what_the_trusted_base_refuses(
    value,
) -> None:
    """`default=str` collided `Fraction(1, 2)` with the string "1/2", and
    the local dialect accepted floats and even emitted a non-standard
    `{"a": NaN}`. There is no stringifying fallback now."""
    with pytest.raises(replay.BatchResourceReplayError):
        replay._local_canonical_json(value)


def test_the_local_digest_takes_exactly_two_values_the_base_cannot() -> None:
    """Booleans and honest absences are the whole of the fork, they are
    encoded explicitly, and they digest stably."""
    first = replay._local_canonical_json(
        {"design_named": True, "shares": None, "n": 3, "s": "x"})
    second = replay._local_canonical_json(
        {"s": "x", "n": 3, "shares": None, "design_named": True})
    assert first == second
    assert replay._local_digest({"a": True}) == replay._local_digest(
        {"a": True})
    # The three are DISTINCT: no encoding collides another value.
    digests = {
        replay._local_digest({"a": True}),
        replay._local_digest({"a": False}),
        replay._local_digest({"a": None}),
        replay._local_digest({"a": "true"}),
        replay._local_digest({"a": 1}),
    }
    assert len(digests) == 5
    # And the reserved marker keys are refused, so the encoding cannot be
    # forged by a report field.
    with pytest.raises(replay.BatchResourceReplayError):
        replay._local_canonical_json({"$e4_bool": "true"})


def test_both_local_digests_go_through_the_one_canonicalizer() -> None:
    """One dialect, defined once — not two forks with different rules."""
    config = configurations()[0]
    workload = hand_built_workloads()[0]
    assert config.digest == replay._local_digest(config.to_canonical_dict())
    assert workload.digest == replay._local_digest(
        workload.to_canonical_dict())
    # No stringifying fallback survives in CODE. The phrase appears once
    # more, inside the canonicalizer's docstring, saying why it is gone.
    source = inspect.getsource(replay)
    assert "default=str)" not in source
    assert "default=str," not in source
    assert "``default=str`` fallback collides" in source
    # And exactly one json.dumps carries a digest in this module: the
    # canonicalizer's. The rest render the manifest and the report.
    assert source.count("json.dumps") == 3


# --------------------------------------------------------------------------- #
# EWD-BRR-6: a non-design comparator is flagged in the row itself.             #
# --------------------------------------------------------------------------- #
def test_every_comparison_row_flags_a_non_design_comparator(report) -> None:
    """Two of the eight matched pools are additions, and a reader of the
    comparison table could not tell."""
    by_name = {c.name: c for c in configurations()}
    added = 0
    for row in report.comparison.per_config:
        assert row.config_design_named == by_name[row.config].design_named
        assert row.matched_shared_design_named == (
            by_name[row.matched_shared].design_named)
        if not row.matched_shared_design_named:
            added += 1
            assert row.matched_shared in {"shared_pool_3", "shared_pool_6"}
        rendered = row.to_canonical_dict()
        assert "matched_shared_design_named" in rendered
    assert added == 4, "the (1,2) and (2,4) splits, borrowing off and on"


# --------------------------------------------------------------------------- #
# EWD-BRR-8: a collected §11 E4 metric reaches the comparison.                 #
# --------------------------------------------------------------------------- #
def test_mandatory_delay_is_compared_across_the_matched_pair(report) -> None:
    """§11 E4 names mandatory prerequisite delay. It was computed per row,
    reported per row, and then never compared — hiding a real weak
    dominance of the borrowing-ON lanes behind the makespan losses."""
    by_key = {(o.config, o.workload, o.candidate): o for o in report.outcomes}
    pairs = capacity_matched_pairs()
    for row in report.comparison.per_config:
        shared = pairs[row.config]
        better = tied = worse = 0
        for (config, workload, candidate), outcome in by_key.items():
            if config != row.config:
                continue
            base = by_key.get((shared, workload, candidate))
            if base is None or base.objective is None:
                continue
            if (outcome.status != STATUS_CHECKED_FEASIBLE
                    or base.status != STATUS_CHECKED_FEASIBLE
                    or outcome.objective is None):
                continue
            lane = outcome.mandatory_delay_total or 0
            pool = base.mandatory_delay_total or 0
            if lane < pool:
                better += 1
            elif lane == pool:
                tied += 1
            else:
                worse += 1
        assert row.mandatory_delay_better == better, row.config
        assert row.mandatory_delay_tied == tied, row.config
        assert row.mandatory_delay_worse == worse, row.config
        assert (better + tied + worse) == row.comparable


def test_the_secondary_metric_decides_nothing(report, manifest) -> None:
    """It is reported BECAUSE it is a signal, and marked because it is
    not the preregistered decider."""
    assert "mandatory_prerequisite_delay_paired_comparison" in (
        manifest.secondary_metrics)
    assert "stages 1-4" in manifest.primary_metric
    for row in report.comparison.per_config:
        rendered = row.to_canonical_dict()["secondary_mandatory_delay"]
        assert rendered["decides"] == "nothing; see manifest.secondary_metrics"
    # The derived `met` is a function of the PRIMARY counts alone: moving
    # every secondary count cannot move it.
    moved = replace(
        report.comparison,
        per_config=tuple(
            replace(row, mandatory_delay_better=row.comparable,
                    mandatory_delay_tied=0, mandatory_delay_worse=0)
            for row in report.comparison.per_config))
    assert moved.met == report.comparison.met
    assert moved.conclusion == report.comparison.conclusion
    # And the borrowing-ON weak dominance is now visible in the report.
    on_rows = [row for row in report.comparison.per_config
               if row.config.endswith("borrow_on_asymmetric")]
    assert on_rows
    assert all(row.mandatory_delay_worse == 0 for row in on_rows)
    assert any(row.mandatory_delay_better > 0 for row in on_rows)
    assert "mandatory-prerequisite-delay" in report.comparison.headline
