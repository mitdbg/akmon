"""Deterministic UNTRUSTED baseline planners — proposal generators only.

PACT ``concord-disclosure-budget``, task.batch.contract-checker
(req.batch.checker-implementation). Normative section:
``designs/batch-scheduling-research.md`` §11 E2's baseline list, with §9
for the trust boundary.

Read this first: nothing in this module is trusted
--------------------------------------------------
These are *proposal generators*. A plan produced here is a claim and
nothing more; it becomes meaningful only after
:func:`...batch_schedule_checker.check_schedule` accepts it. §9 puts
solvers and planners outside the trusted base, and this module is on
that side of the line. Two consequences are structural, not stylistic:

* :mod:`.batch_schedule_checker` must never import this module. A test
  asserts that over the import graph.
* These planners may be wrong. A baseline that emits an infeasible plan
  is not a bug in the checker — it is the checker doing its job, and the
  result is a NAMED infeasibility cause.

They import the trusted base for schema and canonicalization, and the
checker's :func:`~.batch_schedule_checker.derive_may_change` is
deliberately NOT used: each baseline derives its own, coarser disclosure
claim (see :func:`_coarse_may_change`) so that the checker's containment
check remains a real check on baseline output rather than a comparison
of a value against itself.

The baselines, per E2
---------------------
* :func:`plan_fifo` — stable FIFO/discovery order, nothing deferred.
* :func:`plan_dependency_tier_greedy` — dependency-aware tier greedy:
  the mandatory closure first, then ``should``, then ``may``, discovery
  order within a tier.
* :func:`plan_marginal_coverage_greedy` — tier-first marginal DISTINCT
  coverage, then lowest pinned cost, then canonical id. This is the one
  that defers: an action contributing no new coverage unit is left out,
  which is exactly the case that exercises disclosure.

"Marginal distinct coverage" is measured in coverage units (§6), so a
cloned or split obligation adds no marginal credit and cannot pull an
action into the plan.

Determinism
-----------
No randomness, no clock, no set iteration order escapes into output:
every ordering is an explicit sort, so the same instance replays to a
byte-equal plan. The seeded-random diagnostic baseline of E2 is out of
scope for this slice precisely because it would need a seed contract of
its own.

Unknown quantities REFUSE
-------------------------
A baseline cannot place an interval it does not know, and it must never
treat an unknown as zero, so :class:`BaselineRefused` is raised with a
named code. Handing the checker a plan built on a pretend-zero would be
worse than refusing: it might pass.
"""
from __future__ import annotations

from typing import Any, Sequence

from okg.substrate.consistency.batch_schedule import (
    CONTRACT_SCHEMA_VERSION,
    OUTCOME_SUCCESS,
    PRIORITY_MAY,
    PRIORITY_MUST,
    PRIORITY_SHOULD,
    Action,
    BatchInstance,
    CoverageWitness,
    Effects,
    ObjectiveTuple,
    Revelation,
    ScheduledAction,
    SchedulePlan,
    Unknown,
    canonical_schedule_serialization,
)
from okg.substrate.errors import SubstrateError

__all__ = [
    "BASELINES",
    "REFUSE_UNKNOWN_QUANTITY",
    "REFUSE_UNPLACEABLE",
    "BaselineRefused",
    "plan_dependency_tier_greedy",
    "plan_fifo",
    "plan_marginal_coverage_greedy",
]

REFUSE_UNKNOWN_QUANTITY = "unknown_quantity"
REFUSE_UNPLACEABLE = "unplaceable"


class BaselineRefused(SubstrateError):
    """A baseline will not propose a plan. ``data['code']`` names why."""

    code = "batch_schedule_baseline_refused"


def _refuse(code: str, message: str, **extra: Any) -> None:
    raise BaselineRefused(
        f"batch schedule baseline refused [{code}]: {message}",
        data={"code": code, **extra},
    )


def _known_duration(action: Action) -> int:
    if isinstance(action.duration, Unknown):
        _refuse(REFUSE_UNKNOWN_QUANTITY,
                f"action {action.id!r} has an UNKNOWN duration; a baseline "
                "will not invent one, and an unknown is not a zero",
                action=action.id, quantity="duration")
    assert isinstance(action.duration, int)
    return action.duration


def _known_cost(action: Action) -> int:
    if isinstance(action.execution_cost, Unknown):
        _refuse(REFUSE_UNKNOWN_QUANTITY,
                f"action {action.id!r} has an UNKNOWN execution cost",
                action=action.id, quantity="execution_cost")
    assert isinstance(action.execution_cost, int)
    return action.execution_cost


def _known_demand(action: Action, resource_id: str) -> int:
    value = action.demand.get(resource_id, 0)
    if isinstance(value, Unknown):
        _refuse(REFUSE_UNKNOWN_QUANTITY,
                f"action {action.id!r} has an UNKNOWN demand on "
                f"{resource_id!r}",
                action=action.id, quantity=f"demand.{resource_id}")
    assert isinstance(value, int)
    return value


# --------------------------------------------------------------------------- #
# Placement: a naive earliest-feasible integer scan. Untrusted by design.      #
# --------------------------------------------------------------------------- #
def _horizon(instance: BatchInstance) -> int:
    total = sum(_known_duration(a) for a in instance.actions)
    breakpoints = [s.start for r in instance.resources for s in r.segments]
    return total + (max(breakpoints) if breakpoints else 0) + 1


def _fits(
    instance: BatchInstance,
    action: Action,
    start: int,
    placed: dict[str, ScheduledAction],
) -> bool:
    """Would starting ``action`` at ``start`` respect conflicts and capacity?

    A naive integer scan over the candidate interval. Small instances
    only; the checker, not this routine, is the authority on whether the
    resulting plan is feasible."""
    duration = _known_duration(action)
    end = start + duration
    conflicting = set(action.conflicts)
    for other in instance.actions:
        if action.id in other.conflicts:
            conflicting.add(other.id)
    for other_id in sorted(conflicting):
        other = placed.get(other_id)
        if other is not None and start < other.end and other.start < end:
            return False
    for group in instance.conflict_groups:
        if action.id not in group.members:
            continue
        for time in range(start, max(end, start + 1)):
            running = sum(
                1 for member in group.members
                if member in placed
                and placed[member].start <= time < placed[member].end
            )
            if running + 1 > group.max_concurrent:
                return False
    for resource in instance.resources:
        demand = _known_demand(action, resource.id)
        if demand == 0:
            continue
        for time in range(start, max(end, start + 1)):
            capacity = resource.capacity_at(time)
            if isinstance(capacity, Unknown):
                return False
            usage = demand + sum(
                _known_demand(
                    _action_of(instance, scheduled.action_id), resource.id)
                for scheduled in placed.values()
                if scheduled.start <= time < scheduled.end
            )
            if usage > capacity:
                return False
            if isinstance(resource.hard_cap, int) and usage > resource.hard_cap:
                return False
    return True


def _action_of(instance: BatchInstance, action_id: str) -> Action:
    action = instance.action_by_id(action_id)
    assert action is not None
    return action


def _place(
    instance: BatchInstance, order: Sequence[str]
) -> tuple[ScheduledAction, ...]:
    """Place each action in ``order`` at the earliest feasible start."""
    placed: dict[str, ScheduledAction] = {}
    horizon = _horizon(instance)
    for action_id in order:
        action = _action_of(instance, action_id)
        earliest = 0
        for prerequisite in action.requires:
            scheduled = placed.get(prerequisite)
            if scheduled is None:
                _refuse(REFUSE_UNPLACEABLE,
                        f"action {action_id!r} was ordered before its "
                        f"prerequisite {prerequisite!r}",
                        action=action_id, prerequisite=prerequisite)
            else:
                earliest = max(earliest, scheduled.end)
        start = earliest
        while start <= horizon and not _fits(instance, action, start, placed):
            start += 1
        if start > horizon:
            _refuse(REFUSE_UNPLACEABLE,
                    f"action {action_id!r} has no feasible start within the "
                    "horizon",
                    action=action_id)
        placed[action_id] = ScheduledAction(
            action_id=action_id,
            start=start,
            end=start + _known_duration(action),
            resource_assignments={
                resource.id: _known_demand(action, resource.id)
                for resource in instance.resources
                if _known_demand(action, resource.id) != 0
            },
            # A baseline proposes work; it does not report execution. It
            # therefore claims the outcome it is planning for, and the
            # checker judges the resulting plan like any other.
            outcome=OUTCOME_SUCCESS,
        )
    return tuple(placed[action_id] for action_id in order)


# --------------------------------------------------------------------------- #
# The baseline's own, deliberately coarser disclosure claim.                   #
# --------------------------------------------------------------------------- #
def _coarse_may_change(
    instance: BatchInstance, deferred: Sequence[str]
) -> tuple[str, ...]:
    """A conservative superset of the checker's MayChange closure.

    Same closure rules — deferred actions, their transitive
    prerequisites, and everything their stated revelations reach — but
    one coarser step: if ANY closure member's effects are unknown, the
    whole affected scope goes in, rather than only that member's share.
    Coarser in the safe direction, so the checker's containment check
    passes on merit while the code paths stay independent."""
    closure = {a for a in deferred if instance.action_by_id(a) is not None}
    revealed_obligation_classes: set[str] = set()
    changed = True
    while changed:
        changed = False
        for action_id in sorted(closure):
            action = instance.action_by_id(action_id)
            if action is None:
                continue
            for prerequisite in action.requires:
                if prerequisite not in closure:
                    closure.add(prerequisite)
                    changed = True
            if isinstance(action.reveals, Revelation):
                for action_class in action.reveals.action_classes:
                    for other in instance.actions:
                        if (other.action_class == action_class
                                and other.id not in closure):
                            closure.add(other.id)
                            changed = True
                for obligation_class in action.reveals.obligation_classes:
                    if obligation_class not in revealed_obligation_classes:
                        revealed_obligation_classes.add(obligation_class)
                        changed = True
        for obligation_class in sorted(revealed_obligation_classes):
            for obligation in instance.obligations:
                if obligation.obligation_class != obligation_class:
                    continue
                for other in instance.actions:
                    if (obligation.id in other.covers
                            and other.id not in closure):
                        closure.add(other.id)
                        changed = True

    locations: set[str] = set()
    any_unknown = False
    for action_id in sorted(closure):
        action = instance.action_by_id(action_id)
        if action is None:
            continue
        if isinstance(action.effects, Effects):
            locations.update(action.effects.locations)
        else:
            any_unknown = True
    if any_unknown:
        locations.update(instance.whole_affected_scope())
    return tuple(sorted(locations))


# --------------------------------------------------------------------------- #
# Plan assembly.                                                               #
# --------------------------------------------------------------------------- #
def _assemble(
    instance: BatchInstance, order: Sequence[str]
) -> SchedulePlan:
    selected = _place(instance, order)
    selected_ids = {s.action_id for s in selected}
    deferred = tuple(sorted(
        a.id for a in instance.actions if a.id not in selected_ids
    ))

    witnesses: list[CoverageWitness] = []
    covered: set[str] = set()
    total_cost = 0
    for action_id in sorted(selected_ids):
        action = _action_of(instance, action_id)
        total_cost += _known_cost(action)
        for obligation_id in action.covers:
            obligation = instance.obligation_by_id(obligation_id)
            assert obligation is not None
            witnesses.append(CoverageWitness(
                unit_key=obligation.unit.key,
                obligation_id=obligation_id,
                action_id=action_id,
                outcome=OUTCOME_SUCCESS,
            ))
            covered.add(obligation.unit.key)

    objective = ObjectiveTuple(
        should_units=len(covered & set(
            instance.units_by_priority(PRIORITY_SHOULD))),
        may_units=len(covered & set(
            instance.units_by_priority(PRIORITY_MAY))),
        total_cost=total_cost,
        makespan=max((s.end for s in selected), default=0),
        tie_break=canonical_schedule_serialization(selected, deferred),
    )
    return SchedulePlan(
        schema_version=CONTRACT_SCHEMA_VERSION,
        instance_id=instance.instance_id,
        selected=tuple(sorted(selected, key=lambda s: s.action_id)),
        deferred=deferred,
        witnesses=tuple(sorted(
            witnesses, key=lambda w: (w.unit_key, w.obligation_id,
                                      w.action_id))),
        may_change=_coarse_may_change(instance, deferred),
        claimed_objective=objective,
    )


def _dependency_order(
    instance: BatchInstance, wanted: Sequence[str]
) -> tuple[str, ...]:
    """Stable topological order over ``wanted`` plus its prerequisite closure.

    Prerequisite closure is mandatory (§7), so pulling an action in
    pulls everything it needs; the order within a ready set is the
    instance's declaration (discovery) order."""
    discovery = [a.id for a in instance.actions]
    needed = set()
    frontier = list(wanted)
    while frontier:
        action_id = frontier.pop()
        if action_id in needed:
            continue
        needed.add(action_id)
        frontier.extend(_action_of(instance, action_id).requires)

    ordered: list[str] = []
    remaining = [a for a in discovery if a in needed]
    while remaining:
        for action_id in remaining:
            prerequisites = _action_of(instance, action_id).requires
            if all(p in ordered for p in prerequisites):
                ordered.append(action_id)
                remaining.remove(action_id)
                break
        else:
            _refuse(REFUSE_UNPLACEABLE,
                    "the prerequisite graph over the wanted set has a cycle",
                    actions=",".join(sorted(remaining)))
    return tuple(ordered)


def plan_fifo(instance: BatchInstance) -> SchedulePlan:
    """Stable FIFO/discovery order; every action selected, none deferred.

    The simplest honest baseline: it does no selection at all, so it is
    the reference point for "did planning buy anything?" """
    return _assemble(instance, _dependency_order(
        instance, [a.id for a in instance.actions]))


def _tier_of(instance: BatchInstance, action: Action) -> int:
    """0 for mandatory-covering, 1 for ``should``, 2 for ``may``, 3 for rest."""
    ranks = {PRIORITY_MUST: 0, PRIORITY_SHOULD: 1, PRIORITY_MAY: 2}
    best = 3
    for obligation_id in action.covers:
        obligation = instance.obligation_by_id(obligation_id)
        if obligation is None:
            continue
        best = min(best, ranks[obligation.priority])
    return best


def plan_dependency_tier_greedy(instance: BatchInstance) -> SchedulePlan:
    """Dependency-aware tier greedy: mandatory closure, then should, then may.

    Selection is still "everything"; the tier only drives ORDER, which
    is what moves mandatory work earlier under a scarce capacity."""
    discovery = {a.id: i for i, a in enumerate(instance.actions)}
    wanted = sorted(
        (a.id for a in instance.actions),
        key=lambda aid: (_tier_of(instance, _action_of(instance, aid)),
                         discovery[aid]),
    )
    ordered: list[str] = []
    for action_id in wanted:
        for needed in _dependency_order(instance, [action_id]):
            if needed not in ordered:
                ordered.append(needed)
    return _assemble(instance, tuple(ordered))


def plan_marginal_coverage_greedy(instance: BatchInstance) -> SchedulePlan:
    """Tier-first marginal DISTINCT coverage, then lowest pinned cost.

    Works one priority tier at a time and never trades across tiers
    (§8's lexicographic order forbids it). Within the active tier it
    repeatedly takes the action whose prerequisite closure adds the most
    NEW coverage units at that tier, breaking ties on the closure's
    total pinned cost and then on the canonical action id. An action
    whose closure adds nothing is left out — this baseline is the one
    that actually defers, so it is the one that exercises disclosure."""
    chosen: list[str] = []
    covered: set[str] = set()

    def closure_units(action_id: str, priority: str) -> set[str]:
        units: set[str] = set()
        for member in _dependency_order(instance, [action_id]):
            for obligation_id in _action_of(instance, member).covers:
                obligation = instance.obligation_by_id(obligation_id)
                if obligation is not None and obligation.priority == priority:
                    units.add(obligation.unit.key)
        return units

    def closure_cost(action_id: str) -> int:
        return sum(
            _known_cost(_action_of(instance, member))
            for member in _dependency_order(instance, [action_id])
            if member not in chosen
        )

    for priority in (PRIORITY_MUST, PRIORITY_SHOULD, PRIORITY_MAY):
        while True:
            candidates = []
            for action in instance.actions:
                if action.id in chosen:
                    continue
                gain = closure_units(action.id, priority) - covered
                if not gain:
                    continue
                candidates.append(
                    (-len(gain), closure_cost(action.id), action.id))
            if not candidates:
                break
            _, _, best_id = min(candidates)
            for member in _dependency_order(instance, [best_id]):
                if member not in chosen:
                    chosen.append(member)
                for obligation_id in _action_of(instance, member).covers:
                    obligation = instance.obligation_by_id(obligation_id)
                    if obligation is not None:
                        covered.add(obligation.unit.key)

    return _assemble(instance, _dependency_order(instance, chosen))


#: The E2 baseline roster implemented in this slice, in report order.
BASELINES = (
    ("fifo", plan_fifo),
    ("dependency_tier_greedy", plan_dependency_tier_greedy),
    ("marginal_coverage_greedy", plan_marginal_coverage_greedy),
)
