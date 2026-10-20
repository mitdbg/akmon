"""PACT ``concord-disclosure-budget``, req.batch.checker-implementation —
the batch contract and independent checker witnesses.

Covers the task's verification clause: canonical instance/plan/certificate
digests, fail-closed unknowns, stable coverage units, the
dependency/conflict/capacity/mandatory checks each exhibited by a
REJECTING case, recomputed objective values, the deterministic tie-break,
adversarial certificate mutations, and the deterministic baselines.

Design references are ``designs/batch-scheduling-research.md`` §§6-9 and
§12. The oracle's own witnesses live in
``test_batch_schedule_oracle.py``.
"""
from __future__ import annotations

import ast
import copy
import json
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable, Sequence

import pytest

from okg.substrate.consistency import batch_schedule_checker
from okg.substrate.consistency.batch_schedule import (
    ARTIFACT_UNBOUND,
    CONTRACT_SCHEMA_VERSION,
    OUTCOME_FAILURE,
    OUTCOME_SUCCESS,
    PRIORITY_MAY,
    PRIORITY_SHOULD,
    TERMINATION_STOPPED,
    BatchInstance,
    BatchLoadRefused,
    CoverageWitness,
    ObjectiveTuple,
    ProducerRef,
    REFUSAL_CONFLICTING_UNIT_PRIORITY,
    REFUSAL_MISSING_FIELD,
    REFUSAL_MIXED_COST_UNITS,
    REFUSAL_NON_CANONICAL_VALUE,
    REFUSAL_UNKNOWN_FIELD,
    REFUSAL_UNKNOWN_PRIORITY,
    ScheduleCertificate,
    ScheduledAction,
    SchedulePlan,
    Unknown,
    canonical_json,
    canonical_schedule_serialization,
    load_batch_instance,
    load_certificate,
    load_plan,
    object_digest,
    objective_sort_key,
)
from okg.substrate.consistency.batch_schedule_baselines import (
    BASELINES,
    plan_fifo,
    plan_marginal_coverage_greedy,
)
from okg.substrate.consistency.batch_schedule_checker import (
    CAUSE_CAPACITY_EXCEEDED,
    CAUSE_CONFLICT_GROUP_VIOLATION,
    CAUSE_CONFLICT_VIOLATION,
    CAUSE_COST_UNKNOWN,
    CAUSE_DEMAND_UNKNOWN,
    CAUSE_DEPENDENCY_CYCLE,
    CAUSE_DURATION_UNKNOWN,
    CAUSE_IDENTITY_PARTITION,
    CAUSE_INSTANCE_DIGEST_MISMATCH,
    CAUSE_MANDATORY_UNIT_UNCOVERED,
    CAUSE_MAY_CHANGE_INCOMPLETE,
    CAUSE_OBJECTIVE_MISMATCH,
    CAUSE_PLAN_DIGEST_MISMATCH,
    CAUSE_PREREQUISITE_NOT_SELECTED,
    CAUSE_RESERVATION_EXCEEDED,
    CAUSE_TEMPORAL_ORDER_VIOLATION,
    CAUSE_UNKNOWN_REVELATION,
    CHECKER_VERSION,
    VERDICT_CHECKED_FEASIBLE,
    VERDICT_REFUSED,
    check_schedule,
    derive_may_change,
)


# --------------------------------------------------------------------------- #
# Fixtures. Every builder returns a FRESH dict so a mutation in one test       #
# cannot leak into another.                                                    #
# --------------------------------------------------------------------------- #
def unit(name: str, *, spec_version: str = "spec/1") -> dict[str, Any]:
    return {
        "spec_version": spec_version,
        "subject_scope_key": f"subject:{name}",
        "satisfaction_condition": f"condition:{name}",
    }


def action_dict(
    action_id: str,
    *,
    action_class: str = "check",
    covers: Sequence[str] = (),
    requires: Sequence[str] = (),
    conflicts: Sequence[str] = (),
    reveals: Any = None,
    effects: Any = None,
    duration: Any = 2,
    execution_cost: Any = 5,
    cost_unit: str = "tokens",
    demand: Any = None,
) -> dict[str, Any]:
    return {
        "id": action_id,
        "action_class": action_class,
        "covers": list(covers),
        "requires": list(requires),
        "conflicts": list(conflicts),
        "reveals": reveals if reveals is not None
        else {"obligation_classes": [], "action_classes": []},
        "effects": effects if effects is not None
        else {"locations": [f"loc:{action_id}"]},
        "duration": duration,
        "execution_cost": execution_cost,
        "cost_unit": cost_unit,
        "demand": {"cpu": 1} if demand is None else demand,
        "estimator": {"id": "pinned-estimator", "version": 1},
    }


def instance_dict() -> dict[str, Any]:
    """The base instance: one must, one should, one may, one cpu pool."""
    return {
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "instance_id": "batch-1",
        "epoch": 0,
        "cost_unit": "tokens",
        "unit_mapping_version": "map/1",
        "changes": [{
            "id": "c1",
            "affected_scope": ["loc:a1", "loc:a2", "loc:a3", "loc:elsewhere"],
        }],
        "obligations": [
            {"id": "o_must", "obligation_class": "mandatory",
             "priority": "must", "unit": unit("must")},
            {"id": "o_should", "obligation_class": "preferred",
             "priority": "should", "unit": unit("should")},
            {"id": "o_may", "obligation_class": "opportunistic",
             "priority": "may", "unit": unit("may")},
        ],
        "actions": [
            action_dict("a1", action_class="check", covers=["o_must"]),
            action_dict("a2", action_class="repair", covers=["o_should"],
                        requires=["a1"], duration=3, execution_cost=7),
            action_dict("a3", action_class="sweep", covers=["o_may"],
                        duration=1, execution_cost=2),
        ],
        "conflict_groups": [],
        "resources": [{
            "id": "cpu", "version": 1,
            "segments": [{"start": 0, "capacity": 2}],
            "reservation": 1000, "hard_cap": 5,
        }],
    }


def build(mutate: Callable[[dict[str, Any]], None] | None = None
          ) -> BatchInstance:
    raw = instance_dict()
    if mutate is not None:
        mutate(raw)
    return load_batch_instance(raw)


def _assigned(action: Any, resource_id: str) -> int:
    """What a planner would write down for a resource assignment.

    An UNKNOWN demand yields ``1`` here on purpose: a planner cannot
    assign what it does not know, and the test's point is that the
    CHECKER refuses rather than accepting whatever number appears."""
    value = action.demand.get(resource_id, 0)
    return 1 if isinstance(value, Unknown) else int(value)


def make_plan(
    instance: BatchInstance,
    placements: Sequence[tuple[str, int]],
    *,
    may_change: Sequence[str] | None = None,
    outcomes: dict[str, str] | None = None,
    action_outcomes: dict[str, str] | None = None,
) -> SchedulePlan:
    """Hand-build a plan with an HONEST claimed tuple.

    Tests that need a dishonest claim mutate the result, so the honest
    construction here keeps each rejection attributable to exactly the
    thing the test changed.

    ``outcomes`` sets a WITNESS outcome per obligation id;
    ``action_outcomes`` sets the selected ACTION's outcome. A test that
    sets an action to ``failure`` gets witnesses that agree with it, so
    the refusal under test is the missing coverage rather than a
    self-contradictory plan."""
    outcomes = outcomes or {}
    action_outcomes = action_outcomes or {}
    selected: list[ScheduledAction] = []
    witnesses: list[CoverageWitness] = []
    covered: set[str] = set()
    total_cost = 0
    for action_id, start in placements:
        action = instance.action_by_id(action_id)
        assert action is not None
        assert isinstance(action.duration, int)
        action_outcome = action_outcomes.get(action_id, OUTCOME_SUCCESS)
        selected.append(ScheduledAction(
            action_id=action_id,
            start=start,
            end=start + action.duration,
            resource_assignments={
                r.id: _assigned(action, r.id)
                for r in instance.resources
                if _assigned(action, r.id) != 0
            },
            outcome=action_outcome,
        ))
        if isinstance(action.execution_cost, int):
            total_cost += action.execution_cost
        for obligation_id in action.covers:
            obligation = instance.obligation_by_id(obligation_id)
            assert obligation is not None
            outcome = outcomes.get(
                obligation_id,
                OUTCOME_SUCCESS if action_outcome == OUTCOME_SUCCESS
                else action_outcome)
            witnesses.append(CoverageWitness(
                unit_key=obligation.unit.key,
                obligation_id=obligation_id,
                action_id=action_id,
                outcome=outcome,
            ))
            if outcome == OUTCOME_SUCCESS:
                covered.add(obligation.unit.key)
    placed = {action_id for action_id, _ in placements}
    deferred = tuple(sorted(a.id for a in instance.actions
                            if a.id not in placed))
    objective = ObjectiveTuple(
        should_units=len(covered & set(
            instance.units_by_priority(PRIORITY_SHOULD))),
        may_units=len(covered & set(instance.units_by_priority(PRIORITY_MAY))),
        total_cost=total_cost,
        makespan=max((s.end for s in selected), default=0),
        tie_break=canonical_schedule_serialization(selected, deferred),
    )
    if may_change is None:
        may_change = derive_may_change(instance, deferred).locations
    return SchedulePlan(
        schema_version=CONTRACT_SCHEMA_VERSION,
        instance_id=instance.instance_id,
        selected=tuple(selected),
        deferred=deferred,
        witnesses=tuple(witnesses),
        may_change=tuple(may_change),
        claimed_objective=objective,
    )


def hand_producer(
    *, termination_status: str = TERMINATION_STOPPED
) -> ProducerRef:
    """The §12 producer identity of a hand-built test plan."""
    return ProducerRef(
        family="hand-built-fixture",
        version="test/1",
        parameters_digest=object_digest({"source": "test fixture"}),
        seed=Unknown("hand-built fixtures are not seeded"),
        termination_status=termination_status,
    )


def certify(
    instance: BatchInstance,
    plan: SchedulePlan,
    *,
    producer: ProducerRef | None = None,
    optimality_artifact_digest: str = ARTIFACT_UNBOUND,
) -> ScheduleCertificate:
    return ScheduleCertificate(
        schema_version=CONTRACT_SCHEMA_VERSION,
        instance_digest=instance.digest,
        unit_mapping_digest=instance.unit_mapping_digest,
        plan_digest=plan.digest,
        claimed_objective=plan.claimed_objective,
        checker_version=CHECKER_VERSION,
        producer=producer if producer is not None else hand_producer(),
        optimality_artifact_digest=optimality_artifact_digest,
    )


def full_plan(instance: BatchInstance) -> SchedulePlan:
    """The base instance's canonical feasible plan: a1, a3, then a2."""
    return make_plan(instance, [("a1", 0), ("a3", 0), ("a2", 2)])


def check(instance: BatchInstance, plan: SchedulePlan):
    return check_schedule(instance, plan, certify(instance, plan))


# --------------------------------------------------------------------------- #
# 1. Canonical digests.                                                        #
# --------------------------------------------------------------------------- #
def test_instance_round_trips_byte_stably() -> None:
    instance = build()
    rendered = canonical_json(instance.to_canonical_dict())
    reloaded = load_batch_instance(json.loads(rendered))
    assert canonical_json(reloaded.to_canonical_dict()) == rendered
    assert reloaded.digest == instance.digest


def test_plan_round_trips_byte_stably() -> None:
    instance = build()
    plan = full_plan(instance)
    rendered = canonical_json(plan.to_canonical_dict())
    reloaded = load_plan(json.loads(rendered))
    assert canonical_json(reloaded.to_canonical_dict()) == rendered
    assert reloaded.digest == plan.digest


def test_two_loads_agree_on_every_digest() -> None:
    first, second = build(), build()
    assert first.digest == second.digest
    assert first.unit_mapping_digest == second.unit_mapping_digest


def test_declaration_order_does_not_move_the_digest() -> None:
    """Digests are taken over a canonically SORTED rendering, so FIFO
    order stays expressible without making identity order-dependent."""
    def reorder(raw: dict[str, Any]) -> None:
        raw["actions"] = list(reversed(raw["actions"]))
        raw["obligations"] = list(reversed(raw["obligations"]))
    assert build(reorder).digest == build().digest


@pytest.mark.parametrize("mutate", [
    pytest.param(lambda raw: raw["actions"][0].__setitem__("duration", 99),
                 id="duration"),
    pytest.param(lambda raw: raw["actions"][0].__setitem__(
        "execution_cost", 99), id="cost"),
    pytest.param(lambda raw: raw["actions"][1]["requires"].clear(),
                 id="dependency"),
    pytest.param(lambda raw: raw["resources"][0]["segments"][0].__setitem__(
        "capacity", 4), id="capacity"),
    pytest.param(lambda raw: raw["obligations"][1].__setitem__(
        "priority", "may"), id="priority"),
    pytest.param(lambda raw: raw["actions"][0]["effects"]["locations"].append(
        "loc:extra"), id="effects"),
])
def test_any_field_change_changes_the_instance_digest(
    mutate: Callable[[dict[str, Any]], None],
) -> None:
    assert build(mutate).digest != build().digest


def test_unit_mapping_digest_is_its_own_input() -> None:
    """A clone changes the mapping digest even though the UNIT set does
    not — the mapping is a plan input in its own right (§6)."""
    def clone(raw: dict[str, Any]) -> None:
        twin = copy.deepcopy(raw["obligations"][1])
        twin["id"] = "o_should_clone"
        raw["obligations"].append(twin)
        raw["actions"][1]["covers"].append("o_should_clone")
    assert build(clone).unit_mapping_digest != build().unit_mapping_digest


def test_floats_and_nulls_are_not_canonical() -> None:
    with pytest.raises(BatchLoadRefused) as excinfo:
        canonical_json({"rate": 0.5})
    assert excinfo.value.data["code"] == REFUSAL_NON_CANONICAL_VALUE
    with pytest.raises(BatchLoadRefused) as excinfo:
        canonical_json({"value": None})
    assert excinfo.value.data["code"] == REFUSAL_NON_CANONICAL_VALUE


def test_unknown_field_refuses() -> None:
    with pytest.raises(BatchLoadRefused) as excinfo:
        build(lambda raw: raw.__setitem__("lane_policy", "critical"))
    assert excinfo.value.data["code"] == REFUSAL_UNKNOWN_FIELD


# --------------------------------------------------------------------------- #
# 2. Fail-closed unknowns.                                                     #
# --------------------------------------------------------------------------- #
def test_unmapped_priority_refuses_and_never_guesses() -> None:
    with pytest.raises(BatchLoadRefused) as excinfo:
        build(lambda raw: raw["obligations"][0].__setitem__(
            "priority", "block_on_error"))
    assert excinfo.value.data["code"] == REFUSAL_UNKNOWN_PRIORITY


def test_mixed_cost_units_refuse() -> None:
    with pytest.raises(BatchLoadRefused) as excinfo:
        build(lambda raw: raw["actions"][1].__setitem__(
            "cost_unit", "operator_minutes"))
    assert excinfo.value.data["code"] == REFUSAL_MIXED_COST_UNITS


def test_conflicting_unit_priority_refuses() -> None:
    def split_with_disagreement(raw: dict[str, Any]) -> None:
        twin = copy.deepcopy(raw["obligations"][0])
        twin["id"] = "o_must_split"
        twin["priority"] = "should"
        raw["obligations"].append(twin)
    with pytest.raises(BatchLoadRefused) as excinfo:
        build(split_with_disagreement)
    assert excinfo.value.data["code"] == REFUSAL_CONFLICTING_UNIT_PRIORITY


def test_unknown_duration_blocks_the_interval_check_and_is_not_zero() -> None:
    instance = build(lambda raw: raw["actions"][2].__setitem__(
        "duration", {"unknown": "never measured"}))
    plan = make_plan(instance, [("a1", 0), ("a2", 2)])
    plan = replace(
        plan,
        selected=plan.selected + (
            ScheduledAction("a3", 0, 0, {"cpu": 1}, OUTCOME_SUCCESS),),
        deferred=(),
    )
    plan = replace(plan, claimed_objective=replace(
        plan.claimed_objective,
        tie_break=canonical_schedule_serialization(plan.selected, ())))
    result = check(instance, plan)
    assert CAUSE_DURATION_UNKNOWN in result.causes()


def test_unknown_cost_blocks_the_objective() -> None:
    instance = build(lambda raw: raw["actions"][0].__setitem__(
        "execution_cost", {"unknown": "estimator had no trace"}))
    result = check(instance, full_plan(instance))
    assert CAUSE_COST_UNKNOWN in result.causes()
    assert result.recomputed_objective is None


def test_unknown_demand_blocks_the_capacity_check() -> None:
    instance = build(lambda raw: raw["actions"][0].__setitem__(
        "demand", {"cpu": {"unknown": "unprofiled"}}))
    plan = make_plan(instance, [("a1", 0), ("a3", 0), ("a2", 2)])
    result = check(instance, plan)
    assert CAUSE_DEMAND_UNKNOWN in result.causes()


def test_unknown_reservation_is_reported_as_not_established() -> None:
    instance = build(lambda raw: raw["resources"][0].__setitem__(
        "reservation", {"unknown": "pool has no enforceable budget"}))
    result = check(instance, full_plan(instance))
    assert result.verdict == VERDICT_CHECKED_FEASIBLE
    assert "reservation:cpu" in result.not_established


def test_unknown_effect_widens_may_change_to_the_whole_scope() -> None:
    def unknown_effect(raw: dict[str, Any]) -> None:
        raw["actions"][2]["effects"] = {"unknown": "summary not produced"}
    instance = build(unknown_effect)
    derivation = derive_may_change(instance, ["a3"])
    assert derivation.widened_by == ("a3",)
    assert set(derivation.locations) == set(instance.whole_affected_scope())


def test_unknown_revelation_refuses_qualified_publication() -> None:
    def unknown_revelation(raw: dict[str, Any]) -> None:
        raw["actions"][2]["reveals"] = {"unknown": "unbounded envelope"}
    instance = build(unknown_revelation)
    plan = make_plan(instance, [("a1", 0), ("a2", 2)])
    result = check(instance, plan)
    assert CAUSE_UNKNOWN_REVELATION in result.causes()
    assert result.verdict == VERDICT_REFUSED


# --------------------------------------------------------------------------- #
# 3. Coverage units are stable equivalence classes (§6).                       #
# --------------------------------------------------------------------------- #
def clone_should_obligation(raw: dict[str, Any]) -> None:
    """Three obligations, one coverage unit: the E3 clone/split mutation."""
    for suffix in ("clone", "split"):
        twin = copy.deepcopy(raw["obligations"][1])
        twin["id"] = f"o_should_{suffix}"
        raw["obligations"].append(twin)
        raw["actions"][1]["covers"].append(twin["id"])


def test_cloned_obligations_mint_no_extra_should_credit() -> None:
    base = build()
    cloned = build(clone_should_obligation)
    assert len(cloned.obligations) == 5
    assert cloned.units_by_priority(PRIORITY_SHOULD) == \
        base.units_by_priority(PRIORITY_SHOULD)

    base_result = check(base, full_plan(base))
    cloned_result = check(cloned, full_plan(cloned))
    assert base_result.verdict == VERDICT_CHECKED_FEASIBLE
    assert cloned_result.verdict == VERDICT_CHECKED_FEASIBLE
    assert cloned_result.recomputed_objective is not None
    assert base_result.recomputed_objective is not None
    assert cloned_result.recomputed_objective.should_units == \
        base_result.recomputed_objective.should_units == 1


def test_split_mandatory_obligations_are_one_feasibility_unit() -> None:
    """Splitting a `must` must not make feasibility harder or easier: it
    is the same unit, discharged by the same successful witness."""
    def split_must(raw: dict[str, Any]) -> None:
        twin = copy.deepcopy(raw["obligations"][0])
        twin["id"] = "o_must_split"
        raw["obligations"].append(twin)
        raw["actions"][0]["covers"].append("o_must_split")
    instance = build(split_must)
    assert len(instance.units_by_priority("must")) == 1
    assert check(instance, full_plan(instance)).verdict == \
        VERDICT_CHECKED_FEASIBLE


# --------------------------------------------------------------------------- #
# 4. One REJECTING case per checker dimension.                                 #
# --------------------------------------------------------------------------- #
def test_base_plan_is_accepted() -> None:
    """The control. Every rejection below must be attributable to its own
    mutation, which is only meaningful if the unmutated case passes."""
    result = check(build(), full_plan(build()))
    assert result.verdict == VERDICT_CHECKED_FEASIBLE, result.findings
    assert result.findings == ()


def test_missing_mandatory_coverage_is_refused() -> None:
    instance = build()
    plan = make_plan(instance, [("a1", 0), ("a3", 0), ("a2", 2)],
                     outcomes={"o_must": "failure"})
    result = check(instance, plan)
    assert CAUSE_MANDATORY_UNIT_UNCOVERED in result.causes()


def test_unknown_witness_outcome_does_not_discharge_a_must() -> None:
    instance = build()
    plan = make_plan(instance, [("a1", 0), ("a3", 0), ("a2", 2)],
                     outcomes={"o_must": "unknown"})
    assert CAUSE_MANDATORY_UNIT_UNCOVERED in check(instance, plan).causes()


def test_broken_prerequisite_closure_is_refused() -> None:
    instance = build()
    plan = make_plan(instance, [("a1", 0), ("a3", 0), ("a2", 2)])
    # Drop a1 from the selection while keeping its dependent a2.
    plan = replace(
        plan,
        selected=tuple(s for s in plan.selected if s.action_id != "a1"),
        deferred=("a1",),
    )
    result = check(instance, plan)
    assert CAUSE_PREREQUISITE_NOT_SELECTED in result.causes()


def test_temporal_order_violation_is_refused() -> None:
    instance = build()
    plan = make_plan(instance, [("a1", 3), ("a3", 0), ("a2", 0)])
    assert CAUSE_TEMPORAL_ORDER_VIOLATION in check(instance, plan).causes()


def test_dependency_cycle_is_refused() -> None:
    instance = build(lambda raw: raw["actions"][0]["requires"].append("a2"))
    plan = make_plan(instance, [("a1", 0), ("a3", 0), ("a2", 2)])
    assert CAUSE_DEPENDENCY_CYCLE in check(instance, plan).causes()


def test_pairwise_conflict_violation_is_refused() -> None:
    instance = build(lambda raw: raw["actions"][2]["conflicts"].append("a1"))
    plan = make_plan(instance, [("a1", 0), ("a3", 0), ("a2", 2)])
    assert CAUSE_CONFLICT_VIOLATION in check(instance, plan).causes()


def test_pairwise_conflict_is_symmetric_when_declared_on_one_side() -> None:
    """Declared on the HIGHER id only, so a lower-id-only scan would miss
    it. That asymmetry was a real defect during implementation."""
    instance = build(lambda raw: raw["actions"][0]["conflicts"].append("a3"))
    plan = make_plan(instance, [("a1", 0), ("a3", 0), ("a2", 2)])
    assert CAUSE_CONFLICT_VIOLATION in check(instance, plan).causes()


def test_conflict_group_violation_is_refused() -> None:
    def add_group(raw: dict[str, Any]) -> None:
        raw["conflict_groups"].append(
            {"id": "g1", "members": ["a1", "a3"], "max_concurrent": 1})
    instance = build(add_group)
    plan = make_plan(instance, [("a1", 0), ("a3", 0), ("a2", 2)])
    assert CAUSE_CONFLICT_GROUP_VIOLATION in check(instance, plan).causes()


def test_capacity_is_checked_at_an_interior_breakpoint() -> None:
    """The capacity drop happens at t=3, strictly inside both intervals,
    and no action starts or ends there. Evaluating only at start times
    would call this plan feasible."""
    def drop_capacity(raw: dict[str, Any]) -> None:
        raw["actions"] = [
            action_dict("a1", covers=["o_must"], duration=5),
            action_dict("a2", covers=["o_should"], duration=5),
            action_dict("a3", covers=["o_may"], duration=1),
        ]
        raw["resources"][0]["segments"] = [
            {"start": 0, "capacity": 2}, {"start": 3, "capacity": 1},
        ]
    instance = build(drop_capacity)
    plan = make_plan(instance, [("a1", 0), ("a2", 0), ("a3", 5)])
    result = check(instance, plan)
    assert CAUSE_CAPACITY_EXCEEDED in result.causes()
    offending = [f for f in result.findings
                 if f.cause == CAUSE_CAPACITY_EXCEEDED]
    assert offending[0].data["time"] == 3
    starts_only = {s.start for s in plan.selected}
    assert 3 not in starts_only


def test_reservation_overrun_is_refused() -> None:
    instance = build(lambda raw: raw["resources"][0].__setitem__(
        "reservation", 3))
    assert CAUSE_RESERVATION_EXCEEDED in check(
        instance, full_plan(instance)).causes()


def test_identity_partition_must_be_complete() -> None:
    instance = build()
    plan = make_plan(instance, [("a1", 0), ("a2", 2)])
    plan = replace(plan, deferred=())  # a3 neither selected nor deferred
    assert CAUSE_IDENTITY_PARTITION in check(instance, plan).causes()


def test_wrong_claimed_tuple_is_refused() -> None:
    instance = build()
    plan = full_plan(instance)
    plan = replace(plan, claimed_objective=replace(
        plan.claimed_objective, should_units=9))
    assert CAUSE_OBJECTIVE_MISMATCH in check(instance, plan).causes()


def test_may_change_missing_a_reachable_location_is_refused() -> None:
    instance = build()
    honest = make_plan(instance, [("a1", 0)])
    assert honest.may_change  # the deferred closure reaches something
    dishonest = replace(honest, may_change=())
    result = check(instance, dishonest)
    assert CAUSE_MAY_CHANGE_INCOMPLETE in result.causes()
    assert set(result.recomputed_may_change) >= {"loc:a2", "loc:a3"}


def test_may_change_follows_a_revelation_edge() -> None:
    """A deferred action that may reveal an action class drags that
    class's effects into the disclosure set."""
    def reveal_sweep(raw: dict[str, Any]) -> None:
        raw["actions"][1]["reveals"] = {
            "obligation_classes": [], "action_classes": ["sweep"]}
    instance = build(reveal_sweep)
    derivation = derive_may_change(instance, ["a2"])
    assert "a3" in derivation.closure
    assert "loc:a3" in derivation.locations


def test_deferred_work_that_may_reveal_a_must_refuses_publication() -> None:
    def reveal_mandatory(raw: dict[str, Any]) -> None:
        raw["actions"][2]["reveals"] = {
            "obligation_classes": ["mandatory"], "action_classes": []}
    instance = build(reveal_mandatory)
    plan = make_plan(instance, [("a1", 0), ("a2", 2)])
    result = check(instance, plan)
    assert batch_schedule_checker.CAUSE_DEFERRED_MAY_REVEAL_MUST in \
        result.causes()


# --------------------------------------------------------------------------- #
# 5. Adversarial mutations after digesting.                                    #
# --------------------------------------------------------------------------- #
MUTATIONS: list[tuple[str, Callable[[dict[str, Any]], None], str]] = [
    ("dependency_edge",
     lambda raw: raw["actions"][2]["requires"].append("a2"),
     CAUSE_TEMPORAL_ORDER_VIOLATION),
    ("conflict",
     lambda raw: raw["actions"][0]["conflicts"].append("a3"),
     CAUSE_CONFLICT_VIOLATION),
    ("capacity",
     lambda raw: raw["resources"][0]["segments"][0].__setitem__(
         "capacity", 1),
     CAUSE_CAPACITY_EXCEEDED),
]


@pytest.mark.parametrize("name,mutate,expected",
                         MUTATIONS, ids=[m[0] for m in MUTATIONS])
def test_instance_mutation_breaks_the_certificate_binding(
    name: str, mutate: Callable[[dict[str, Any]], None], expected: str,
) -> None:
    """Step one: the plan was certified against the honest instance, so
    mutating the instance breaks the digest binding outright."""
    honest = build()
    plan = full_plan(honest)
    certificate = certify(honest, plan)
    mutated = build(mutate)
    assert mutated.digest != honest.digest, \
        "the mutation changed nothing; this test would pass vacuously"
    result = check_schedule(mutated, plan, certificate)
    assert CAUSE_INSTANCE_DIGEST_MISMATCH in result.causes()


@pytest.mark.parametrize("name,mutate,expected",
                         MUTATIONS, ids=[m[0] for m in MUTATIONS])
def test_recertified_mutation_is_caught_by_the_named_constraint(
    name: str, mutate: Callable[[dict[str, Any]], None], expected: str,
) -> None:
    """Step two, the harder case: the adversary also re-digests, so the
    binding is intact and only the re-derived constraint can refute the
    plan. It names exactly what broke."""
    honest = build()
    plan = full_plan(honest)
    mutated = build(mutate)
    assert mutated.digest != honest.digest, \
        "the mutation changed nothing; this test would pass vacuously"
    result = check_schedule(mutated, plan, certify(mutated, plan))
    assert expected in result.causes(), result.findings


def test_objective_mutation_in_the_certificate_is_caught() -> None:
    instance = build()
    plan = full_plan(instance)
    certificate = certify(instance, plan)
    tampered = replace(certificate, claimed_objective=replace(
        certificate.claimed_objective, total_cost=1))
    assert tampered != certificate
    result = check_schedule(instance, plan, tampered)
    assert batch_schedule_checker.CAUSE_CERTIFICATE_OBJECTIVE_MISMATCH in \
        result.causes()


def test_plan_mutation_after_digesting_is_caught() -> None:
    instance = build()
    plan = full_plan(instance)
    certificate = certify(instance, plan)
    tampered = replace(plan, deferred=("a3",), selected=tuple(
        s for s in plan.selected if s.action_id != "a3"))
    assert tampered.digest != plan.digest
    result = check_schedule(instance, tampered, certificate)
    assert CAUSE_PLAN_DIGEST_MISMATCH in result.causes()


# --------------------------------------------------------------------------- #
# 6. Deterministic tie-break and replay.                                       #
# --------------------------------------------------------------------------- #
def test_value_equal_plans_order_by_canonical_serialization() -> None:
    """Two plans identical in stages 1-4 are separated by stage 5 alone,
    and the separation is a total order, not a coin flip."""
    def twins(raw: dict[str, Any]) -> None:
        raw["actions"] = [
            action_dict("a1", covers=["o_must"], duration=2),
            action_dict("a2", covers=["o_should"], duration=2),
            action_dict("a3", covers=["o_may"], duration=2),
        ]
        raw["resources"][0]["segments"] = [{"start": 0, "capacity": 3}]
    instance = build(twins)
    left = check(instance, make_plan(instance, [("a1", 0), ("a2", 0),
                                                ("a3", 0)]))
    right = check(instance, make_plan(instance, [("a1", 0), ("a2", 0),
                                                 ("a3", 0)]))
    assert left.recomputed_objective == right.recomputed_objective

    shifted = check(instance, make_plan(instance, [("a1", 0), ("a2", 0),
                                                   ("a3", 2)]))
    assert shifted.verdict == VERDICT_CHECKED_FEASIBLE
    assert left.recomputed_objective is not None
    assert shifted.recomputed_objective is not None
    ordered = sorted(
        [left.recomputed_objective, shifted.recomputed_objective],
        key=objective_sort_key)
    assert ordered[0].makespan <= ordered[1].makespan
    assert objective_sort_key(left.recomputed_objective) != \
        objective_sort_key(shifted.recomputed_objective)


def test_same_inputs_replay_to_byte_equal_results() -> None:
    first = check(build(), full_plan(build()))
    second = check(build(), full_plan(build()))
    assert first == second
    assert canonical_json(
        first.recomputed_objective.to_canonical_dict()  # type: ignore[union-attr]
    ) == canonical_json(
        second.recomputed_objective.to_canonical_dict()  # type: ignore[union-attr]
    )


# --------------------------------------------------------------------------- #
# 7. Baselines: untrusted proposals the checker then judges.                    #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("name,planner", BASELINES,
                         ids=[name for name, _ in BASELINES])
def test_baseline_emits_a_plan_the_checker_judges(name, planner) -> None:
    instance = build()
    plan = planner(instance)
    result = check(instance, plan)
    assert result.verdict in (VERDICT_CHECKED_FEASIBLE, VERDICT_REFUSED)
    if result.verdict == VERDICT_REFUSED:
        assert result.causes(), "a refusal must name its cause"
    else:
        assert result.recomputed_objective == plan.claimed_objective


@pytest.mark.parametrize("name,planner", BASELINES,
                         ids=[name for name, _ in BASELINES])
def test_baseline_is_deterministic(name, planner) -> None:
    assert planner(build()).digest == planner(build()).digest


def test_fifo_is_feasible_on_the_base_instance() -> None:
    instance = build()
    assert check(instance, plan_fifo(instance)).verdict == \
        VERDICT_CHECKED_FEASIBLE


def test_marginal_greedy_defers_work_that_adds_no_distinct_unit() -> None:
    """An action duplicating an already-covered unit earns no marginal
    coverage, so the coverage-driven baseline leaves it out."""
    def add_redundant(raw: dict[str, Any]) -> None:
        raw["actions"].append(action_dict(
            "a4", action_class="sweep", covers=["o_may"], duration=1,
            execution_cost=50))
    instance = build(add_redundant)
    plan = plan_marginal_coverage_greedy(instance)
    assert "a4" in plan.deferred
    result = check(instance, plan)
    assert result.verdict == VERDICT_CHECKED_FEASIBLE, result.findings


# --------------------------------------------------------------------------- #
# 8. Independence, asserted over the import graph.                             #
# --------------------------------------------------------------------------- #
def module_imports(module_name: str) -> set[str]:
    path = Path(
        __import__(module_name, fromlist=["__file__"]).__file__  # noqa: S101
    )
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def test_checker_imports_no_planner_or_baseline() -> None:
    """§9: the checker shares only schema and canonicalization with any
    planner. Asserted structurally, because a comment cannot enforce it."""
    imports = module_imports(
        "okg.substrate.consistency.batch_schedule_checker")
    forbidden = [name for name in imports
                 if "baseline" in name or "planner" in name
                 or "oracle" in name or "solver" in name]
    assert forbidden == []
    assert imports <= {
        "__future__", "dataclasses", "typing",
        "okg.substrate.consistency.batch_schedule",
    }, imports


def test_trusted_base_stays_dependency_light() -> None:
    imports = module_imports("okg.substrate.consistency.batch_schedule")
    assert imports <= {
        "__future__", "hashlib", "json", "dataclasses", "typing",
        "okg.substrate.errors",
    }, imports


# --------------------------------------------------------------------------- #
# 9. Adversarial-review regressions (EWD-BSC-2, -5, -6, -7).                    #
# --------------------------------------------------------------------------- #
def over_subscribe_cpu(raw: dict[str, Any]) -> None:
    """The EWD-BSC-2 repro shape: capacity 1, with a1 and a3 overlapping."""
    raw["resources"][0]["segments"] = [{"start": 0, "capacity": 1}]
    raw["actions"][1]["requires"] = []


def test_an_omitted_demand_key_refuses_at_load() -> None:
    """EWD-BSC-2. An action silent about a declared resource used to read
    as zero demand, which turned a capacity violation into a clean pass
    with nothing in `not_established` to show for it."""
    def omit_demand(raw: dict[str, Any]) -> None:
        over_subscribe_cpu(raw)
        raw["actions"][2]["demand"] = {}
    with pytest.raises(BatchLoadRefused) as excinfo:
        build(omit_demand)
    assert excinfo.value.data["code"] == REFUSAL_MISSING_FIELD
    assert excinfo.value.data["where"] == "$.actions[a3].demand.cpu"

    # The honest declaration is the contrast the memo drew: it names the
    # violation the omission used to hide.
    honest = build(over_subscribe_cpu)
    result = check(honest, make_plan(honest, [("a1", 0), ("a3", 0),
                                              ("a2", 2)]))
    assert result.verdict == VERDICT_REFUSED
    assert CAUSE_CAPACITY_EXCEEDED in result.causes()


def test_an_omitted_demand_key_refuses_at_checking_too() -> None:
    """The same door, locked on the checker's side: an instance that did
    not come through the loader cannot buy a permissive zero either."""
    instance = build(over_subscribe_cpu)
    stripped = replace(instance.action_by_id("a3"), demand={})  # type: ignore[arg-type]
    instance = replace(instance, actions=tuple(
        stripped if a.id == "a3" else a for a in instance.actions))
    plan = make_plan(instance, [("a1", 0), ("a3", 0), ("a2", 2)])
    result = check(instance, plan)
    assert result.verdict == VERDICT_REFUSED
    assert CAUSE_DEMAND_UNKNOWN in result.causes()


def test_a_zero_duration_action_is_refused_at_load() -> None:
    """EWD-BSC-5. `start <= t < end` is empty when start == end, so a
    zero-duration action used to sit outside every capacity, conflict,
    and reservation check while still collecting its coverage credit."""
    def exempt_a3(raw: dict[str, Any]) -> None:
        raw["resources"][0]["segments"] = [{"start": 0, "capacity": 1}]
        raw["conflict_groups"].append(
            {"id": "g1", "members": ["a1", "a3"], "max_concurrent": 1})
        raw["actions"][2]["duration"] = 0
        raw["actions"][2]["demand"] = {"cpu": 99}
    with pytest.raises(BatchLoadRefused) as excinfo:
        build(exempt_a3)
    assert excinfo.value.data["code"] == REFUSAL_NON_CANONICAL_VALUE
    assert excinfo.value.data["where"] == "$.actions[2].duration"


def test_an_unmeasured_duration_is_still_expressible_as_unknown() -> None:
    """Refusing `0` must not cost the contract its honest sentinel."""
    instance = build(lambda raw: raw["actions"][2].__setitem__(
        "duration", {"unknown": "never measured"}))
    assert isinstance(instance.action_by_id("a3").duration, Unknown)  # type: ignore[union-attr]


def add_failing_chain(raw: dict[str, Any]) -> None:
    """The EWD-BSC-6 repro shape: a4 covers a `should`, a5 requires a4."""
    raw["obligations"].append(
        {"id": "o_should2", "obligation_class": "preferred",
         "priority": "should", "unit": unit("should2")})
    raw["obligations"].append(
        {"id": "o_may2", "obligation_class": "opportunistic",
         "priority": "may", "unit": unit("may2")})
    raw["actions"].append(action_dict(
        "a4", action_class="repair", covers=["o_should2"], duration=1,
        execution_cost=1))
    raw["actions"].append(action_dict(
        "a5", action_class="repair", covers=["o_may2"], requires=["a4"],
        duration=1, execution_cost=1))


CHAIN_PLACEMENTS = [("a1", 0), ("a4", 0), ("a5", 1)]


def test_the_all_succeeding_chain_is_accepted() -> None:
    """The control for the two tests below: without a failure, the same
    placement is feasible and a5's `may` unit IS credited."""
    instance = build(add_failing_chain)
    result = check(instance, make_plan(instance, CHAIN_PLACEMENTS))
    assert result.verdict == VERDICT_CHECKED_FEASIBLE, result.findings
    assert result.recomputed_objective is not None
    assert result.recomputed_objective.may_units == 1


def test_credit_does_not_flow_past_a_failed_prerequisite() -> None:
    """EWD-BSC-6. §7 admits `a` only after prerequisite `b` SUCCEEDS.
    The checker enforced "selected" and "earlier" and nothing about
    success, so a5's unit was credited downstream of a4's failure."""
    instance = build(add_failing_chain)
    plan = make_plan(instance, CHAIN_PLACEMENTS,
                     action_outcomes={"a4": OUTCOME_FAILURE})
    result = check(instance, plan)
    assert result.verdict == VERDICT_REFUSED
    assert batch_schedule_checker.CAUSE_PREREQUISITE_NOT_SUCCEEDED in \
        result.causes()
    assert result.recomputed_objective is not None
    assert result.recomputed_objective.may_units == 0
    assert result.recomputed_objective.should_units == 0


def test_a_prerequisite_that_covers_nothing_can_still_report_failure() -> None:
    """The second half of the memo's repro: an action covering no
    obligation had no slot in which success could be written at all.
    `ScheduledAction.outcome` is per ACTION, so it does now."""
    def chain_without_coverage(raw: dict[str, Any]) -> None:
        add_failing_chain(raw)
        raw["actions"][3]["covers"] = []
    instance = build(chain_without_coverage)
    plan = make_plan(instance, CHAIN_PLACEMENTS,
                     action_outcomes={"a4": OUTCOME_FAILURE})
    assert any(s.outcome == OUTCOME_FAILURE for s in plan.selected)
    result = check(instance, plan)
    assert result.verdict == VERDICT_REFUSED
    assert batch_schedule_checker.CAUSE_PREREQUISITE_NOT_SUCCEEDED in \
        result.causes()


def test_a_witness_cannot_outrank_its_own_action_outcome() -> None:
    """A success witness on a failed action is a contradiction the plan
    does not get to resolve in its own favour."""
    instance = build()
    plan = make_plan(instance, [("a1", 0), ("a3", 0), ("a2", 2)],
                     action_outcomes={"a1": OUTCOME_FAILURE},
                     outcomes={"o_must": OUTCOME_SUCCESS})
    result = check(instance, plan)
    assert batch_schedule_checker.CAUSE_WITNESS_INVALID in result.causes()
    assert CAUSE_MANDATORY_UNIT_UNCOVERED in result.causes()


@pytest.mark.parametrize("mutate", [
    pytest.param(lambda producer: replace(producer, family=""), id="family"),
    pytest.param(lambda producer: replace(producer, version=""), id="version"),
    pytest.param(lambda producer: replace(producer, parameters_digest=""),
                 id="parameters_digest"),
    pytest.param(lambda producer: replace(
        producer, termination_status="solver_said_ok"), id="termination"),
])
def test_a_certificate_that_does_not_bind_its_producer_is_refused(
    mutate: Callable[[ProducerRef], ProducerRef],
) -> None:
    """EWD-BSC-7, first half. §12 requires every certificate to bind who
    produced the plan and how its search terminated."""
    instance = build()
    plan = full_plan(instance)
    certificate = certify(instance, plan, producer=mutate(hand_producer()))
    result = check_schedule(instance, plan, certificate)
    assert result.verdict == VERDICT_REFUSED
    assert batch_schedule_checker.CAUSE_PRODUCER_BINDING_INVALID in \
        result.causes()


def test_a_malformed_artifact_binding_is_refused() -> None:
    instance = build()
    plan = full_plan(instance)
    certificate = certify(instance, plan,
                          optimality_artifact_digest="probably-fine")
    result = check_schedule(instance, plan, certificate)
    assert batch_schedule_checker.CAUSE_PRODUCER_BINDING_INVALID in \
        result.causes()


def test_the_certificate_round_trips_its_new_bindings() -> None:
    """The §12 additions are canonical, digestible, and reloadable."""
    instance = build()
    plan = full_plan(instance)
    certificate = certify(instance, plan)
    rendered = canonical_json(certificate.to_canonical_dict())
    reloaded = load_certificate(json.loads(rendered))
    assert reloaded == certificate
    assert reloaded.digest == certificate.digest
    assert certificate.optimality_artifact_digest == ARTIFACT_UNBOUND


def test_the_tie_break_is_a_schedule_not_an_execution_report() -> None:
    """§8 stage 5 orders SCHEDULES. If it read outcomes, a plan could
    improve its own tie-break by declaring a failure, since 'failure'
    sorts before 'success'."""
    instance = build()
    honest = full_plan(instance)
    failed = make_plan(instance, [("a1", 0), ("a3", 0), ("a2", 2)],
                       action_outcomes={"a3": OUTCOME_FAILURE})
    assert honest.claimed_objective.tie_break == \
        failed.claimed_objective.tie_break
    assert honest.digest != failed.digest
