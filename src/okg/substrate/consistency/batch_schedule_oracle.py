"""The tiny exhaustive oracle: the only producer of an optimality artifact.

PACT ``concord-disclosure-budget``, task.batch.contract-checker
(req.batch.checker-implementation). Normative section:
``designs/batch-scheduling-research.md`` §9.1, "Optimality rule", with
§8 for the objective and §7 for the schedule model.

Why this module exists
----------------------
§9.1 permits the words *optimal* or *certified optimal* only when one of
two artifacts is independently checked: exhaustive enumeration by a
small-instance oracle including the canonical best tuple, or a
solver-independent bound whose value equals the checked candidate at
every objective stage. This module produces the first kind. Nothing
else in this slice may produce one.

What it enumerates, exactly
---------------------------
Every **active schedule** over the instance's canonical event lattice:

* the search starts at time 0 with nothing placed;
* at any state it may place one ready action (all prerequisites placed
  and finished) at the current time, advance to the next event time —
  the next action end or capacity breakpoint — or stop, deferring
  everything not yet placed;
* placing actions one at a time at the same instant reaches every
  simultaneous start, and advancing reaches every start time that any
  active schedule can use.

What the oracle owns, and the one thing it borrows
--------------------------------------------------
The search, the objective claim on every candidate, the comparison
between candidates, and the disclosure claim on every plan are all the
oracle's own. It contains no priority ordering, no greedy rule, and no
planner heuristic of any kind: it explores every branch or it refuses.

The one thing it borrows is FEASIBILITY judgement: each enumerated
candidate is handed to :func:`...batch_schedule_checker.check_schedule`
and kept only if that checker accepts it. That is a deliberate limit of
this artifact and not a safety property. The checker is the trusted
verifier of the whole slice, so a search that used a second, unreviewed
feasibility model would be enumerating a different problem — but the
consequence is real and must travel with any evidence from here: an
oracle plan's feasibility is not independent corroboration of the
checker's feasibility rules.

Disclosure is deliberately NOT borrowed. An earlier version of this
module set each plan's ``may_change`` to
:func:`...batch_schedule_checker.derive_may_change`'s answer, which made
the checker's containment check a comparison of a value against itself:
neutering that derivation broke five tests, none of them here. Now
:func:`_own_may_change` derives the claim from the instance alone, by
the maximally conservative rule §7 licenses for an unknown effect — the
whole affected scope, plus every declared effect location, whenever
anything is deferred. Over-widening is legal (containment is what the
checker tests, and a wider claim only demands more warnings) and it
keeps the containment check a real comparison of two independently
derived sets.

Comparison is :func:`...batch_schedule.objective_sort_key` applied to
the CHECKER's recomputed tuple, never to the candidate's own claim.

The size guard REFUSES; it never caps silently
----------------------------------------------
A silently truncated search that still reported a "best" tuple would be
the exact failure §9.1 exists to prevent — the artifact would assert
exhaustiveness over a space it never covered. So every bound raises
:class:`OracleRefused` with a named code: an instance with more actions
than ``max_actions``, a search that exceeds ``node_budget``, or any
UNKNOWN duration, cost, or demand (which cannot be enumerated over and
must not be read as zero). ``search_space_digest`` on the artifact binds
the bounds and the action set that were actually enumerated.

What an artifact from here does NOT establish
---------------------------------------------
Optimality with respect to the SUPPLIED instance and its pinned
estimates only (§7.1: "The objective is optimal only with respect to the
pinned declared estimates"). It says nothing about dependency
completeness, effect-summary soundness, or whether the estimates
resemble reality.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from okg.substrate.consistency.batch_schedule import (
    ARTIFACT_UNBOUND,
    CONTRACT_SCHEMA_VERSION,
    OPTIMALITY_METHOD_EXHAUSTIVE,
    OUTCOME_SUCCESS,
    PRIORITY_MAY,
    PRIORITY_SHOULD,
    TERMINATION_EXHAUSTED,
    BatchInstance,
    CoverageWitness,
    Effects,
    ObjectiveTuple,
    OptimalityArtifact,
    ProducerRef,
    ScheduleCertificate,
    ScheduledAction,
    SchedulePlan,
    Unknown,
    canonical_schedule_serialization,
    enumeration_breakpoints,
    enumeration_horizon,
    object_digest,
    objective_sort_key,
    search_space_digest,
)
from okg.substrate.consistency.batch_schedule_checker import (
    CHECKER_VERSION,
    ScheduleCheckResult,
    check_schedule,
)
from okg.substrate.errors import SubstrateError

__all__ = [
    "DEFAULT_MAX_ACTIONS",
    "DEFAULT_NODE_BUDGET",
    "ORACLE_FAMILY",
    "ORACLE_VERSION",
    "REFUSE_INSTANCE_TOO_LARGE",
    "REFUSE_NODE_BUDGET_EXCEEDED",
    "REFUSE_NO_FEASIBLE_CANDIDATE",
    "REFUSE_UNKNOWN_QUANTITY",
    "OracleRefused",
    "OracleResult",
    "build_certificate",
    "enumerate_optimum",
    "producer_ref",
]

ORACLE_FAMILY = "exhaustive-oracle"
ORACLE_VERSION = "batch-schedule-oracle/1"

#: The declared bound. "Tiny" is the design's word (§9.1) and E2 sizes
#: its hand-built edge cases to fit it.
DEFAULT_MAX_ACTIONS = 6
DEFAULT_NODE_BUDGET = 200_000

REFUSE_INSTANCE_TOO_LARGE = "instance_too_large"
REFUSE_NODE_BUDGET_EXCEEDED = "node_budget_exceeded"
REFUSE_UNKNOWN_QUANTITY = "unknown_quantity"
REFUSE_NO_FEASIBLE_CANDIDATE = "no_feasible_candidate"


class OracleRefused(SubstrateError):
    """The oracle will not answer. ``data['code']`` names why.

    Refusing is the point: an oracle that quietly narrowed its search
    and still called the survivor "best" would manufacture exactly the
    unearned optimality claim §9.1 forbids."""

    code = "batch_schedule_oracle_refused"


@dataclass(frozen=True)
class OracleResult:
    """The exhaustive result: a best plan, its checked value, its artifact."""

    plan: SchedulePlan
    certificate: ScheduleCertificate
    check: ScheduleCheckResult
    best_objective: ObjectiveTuple
    artifact: OptimalityArtifact
    candidates_examined: int
    feasible_candidates: int


def producer_ref(
    *,
    max_actions: int = DEFAULT_MAX_ACTIONS,
    node_budget: int = DEFAULT_NODE_BUDGET,
    termination_status: str = TERMINATION_EXHAUSTED,
) -> ProducerRef:
    """The oracle's §12 producer identity.

    ``seed`` is UNKNOWN with a reason rather than ``0``: this producer
    has no random seed, and writing one down would be a fiction that
    replay tooling would later try to honour."""
    return ProducerRef(
        family=ORACLE_FAMILY,
        version=ORACLE_VERSION,
        parameters_digest=object_digest(
            {"max_actions": max_actions, "node_budget": node_budget}),
        seed=Unknown("the exhaustive oracle is deterministic and unseeded"),
        termination_status=termination_status,
    )


def build_certificate(
    instance: BatchInstance,
    plan: SchedulePlan,
    *,
    producer: ProducerRef | None = None,
    optimality_artifact_digest: str = ARTIFACT_UNBOUND,
) -> ScheduleCertificate:
    """Bind a plan to its instance, unit mapping, and PRODUCER (§12).

    ``optimality_artifact_digest`` defaults to
    :data:`~.batch_schedule.ARTIFACT_UNBOUND` because most certificates
    stand behind no optimality claim; the winning candidate's
    certificate is rebuilt with the artifact bound once the search has
    actually produced one."""
    return ScheduleCertificate(
        schema_version=CONTRACT_SCHEMA_VERSION,
        instance_digest=instance.digest,
        unit_mapping_digest=instance.unit_mapping_digest,
        plan_digest=plan.digest,
        claimed_objective=plan.claimed_objective,
        checker_version=CHECKER_VERSION,
        producer=producer if producer is not None else producer_ref(),
        optimality_artifact_digest=optimality_artifact_digest,
    )


def _refuse(code: str, message: str, **extra: Any) -> None:
    raise OracleRefused(
        f"batch schedule oracle refused [{code}]: {message}",
        data={"code": code, **extra},
    )


def _require_known_quantities(instance: BatchInstance) -> None:
    for action in sorted(instance.actions, key=lambda a: a.id):
        if isinstance(action.duration, Unknown):
            _refuse(REFUSE_UNKNOWN_QUANTITY,
                    f"action {action.id!r} has an UNKNOWN duration; the "
                    "enumeration cannot place an interval it does not know, "
                    "and an unknown duration is not a zero duration",
                    action=action.id, quantity="duration")
        if isinstance(action.execution_cost, Unknown):
            _refuse(REFUSE_UNKNOWN_QUANTITY,
                    f"action {action.id!r} has an UNKNOWN execution cost; "
                    "no objective stage 3 value exists to compare",
                    action=action.id, quantity="execution_cost")
        for resource_id, demand in sorted(action.demand.items()):
            if isinstance(demand, Unknown):
                _refuse(REFUSE_UNKNOWN_QUANTITY,
                        f"action {action.id!r} has an UNKNOWN demand on "
                        f"resource {resource_id!r}",
                        action=action.id, quantity=f"demand.{resource_id}")


def _own_may_change(
    instance: BatchInstance, deferred: tuple[str, ...]
) -> tuple[str, ...]:
    """The oracle's OWN disclosure claim: maximally conservative.

    §7 says an unknown effect widens disclosure to the whole affected
    scope. This takes the same widening unconditionally whenever anything
    is deferred, and adds every declared effect location so that an
    effect naming a location outside any change's scope is still covered.
    The result is a superset of any closure-based derivation by
    construction, which is what makes the checker's containment check a
    real comparison rather than an identity: the oracle never asks the
    checker what to claim.

    Over-widening costs the oracle nothing here — disclosure is not an
    objective stage — and it errs in the direction §9 requires."""
    if not deferred:
        return ()
    locations: set[str] = set(instance.whole_affected_scope())
    for action in instance.actions:
        if isinstance(action.effects, Effects):
            locations.update(action.effects.locations)
    return tuple(sorted(locations))


def _enumerate_placements(
    instance: BatchInstance, *, node_budget: int
) -> list[tuple[tuple[str, int], ...]]:
    """Every active-schedule placement set, canonically deduplicated.

    A placement set is ``((action_id, start), ...)``; whatever is absent
    is deferred. Exploration is memoized on ``(time, placements)`` so a
    state is expanded once, and the node budget is a REFUSAL, not a cap."""
    durations = {a.id: a.duration for a in instance.actions}
    requires = {a.id: tuple(a.requires) for a in instance.actions}
    horizon = enumeration_horizon(instance)
    breakpoints = enumeration_breakpoints(instance)

    found: dict[tuple[tuple[str, int], ...], None] = {}
    seen: set[tuple[int, tuple[tuple[str, int], ...]]] = set()
    stack: list[tuple[int, tuple[tuple[str, int], ...]]] = [(0, ())]
    nodes = 0

    while stack:
        time, placements = stack.pop()
        state = (time, placements)
        if state in seen:
            continue
        seen.add(state)
        nodes += 1
        if nodes > node_budget:
            _refuse(
                REFUSE_NODE_BUDGET_EXCEEDED,
                f"the enumeration exceeded its declared node budget of "
                f"{node_budget}; the oracle refuses rather than reporting a "
                "best value over a search it did not finish",
                node_budget=node_budget, actions=len(instance.actions))
        found.setdefault(placements, None)

        placed = {action_id: start for action_id, start in placements}
        ends = {
            action_id: start + durations[action_id]
            for action_id, start in placed.items()
        }

        for action_id in sorted(durations):
            if action_id in placed:
                continue
            if any(prereq not in ends or ends[prereq] > time
                   for prereq in requires[action_id]):
                continue
            if time + durations[action_id] > horizon:
                continue
            grown = tuple(sorted(placements + ((action_id, time),)))
            stack.append((time, grown))

        later = sorted(
            t for t in set(ends.values()) | set(breakpoints)
            if time < t <= horizon
        )
        if later:
            stack.append((later[0], placements))

    return sorted(found)


def _build_plan(
    instance: BatchInstance, placements: tuple[tuple[str, int], ...]
) -> SchedulePlan:
    """Assemble a candidate plan, computing its own claimed objective.

    The claim is derived here with the oracle's own arithmetic rather
    than borrowed from the checker, so the checker's objective check
    stays a real check even on oracle-produced plans."""
    durations = {a.id: a.duration for a in instance.actions}
    placed = dict(placements)
    selected = []
    for action_id, start in sorted(placements):
        action = instance.action_by_id(action_id)
        assert action is not None
        assert isinstance(action.duration, int)
        assignments = {
            resource.id: int(action.demand.get(resource.id, 0))
            for resource in instance.resources
            if int(action.demand.get(resource.id, 0)) != 0
        }
        selected.append(ScheduledAction(
            action_id=action_id,
            start=start,
            end=start + action.duration,
            resource_assignments=assignments,
            # A planning oracle plans work it expects to succeed: an
            # enumerated candidate that pre-declares its own failures
            # would be proposing a worse plan for no reason, since a
            # non-success outcome can only remove coverage credit.
            outcome=OUTCOME_SUCCESS,
        ))
    deferred = tuple(sorted(
        a.id for a in instance.actions if a.id not in placed
    ))

    witnesses = []
    covered_units: set[str] = set()
    total_cost = 0
    for action_id, _ in sorted(placements):
        action = instance.action_by_id(action_id)
        assert action is not None
        assert isinstance(action.execution_cost, int)
        total_cost += action.execution_cost
        for obligation_id in action.covers:
            obligation = instance.obligation_by_id(obligation_id)
            assert obligation is not None
            witnesses.append(CoverageWitness(
                unit_key=obligation.unit.key,
                obligation_id=obligation_id,
                action_id=action_id,
                outcome=OUTCOME_SUCCESS,
            ))
            covered_units.add(obligation.unit.key)

    makespan = max(
        (start + durations[action_id] for action_id, start in placements),
        default=0,
    )
    objective = ObjectiveTuple(
        should_units=len(covered_units & set(
            instance.units_by_priority(PRIORITY_SHOULD))),
        may_units=len(covered_units & set(
            instance.units_by_priority(PRIORITY_MAY))),
        total_cost=total_cost,
        makespan=makespan,
        tie_break=canonical_schedule_serialization(selected, deferred),
    )
    return SchedulePlan(
        schema_version=CONTRACT_SCHEMA_VERSION,
        instance_id=instance.instance_id,
        selected=tuple(selected),
        deferred=deferred,
        witnesses=tuple(witnesses),
        may_change=_own_may_change(instance, deferred),
        claimed_objective=objective,
    )


def enumerate_optimum(
    instance: BatchInstance,
    *,
    max_actions: int = DEFAULT_MAX_ACTIONS,
    node_budget: int = DEFAULT_NODE_BUDGET,
) -> OracleResult:
    """Exhaustively enumerate and return the canonical best candidate.

    Ties at every objective stage are broken by stage 5 — the
    lexicographically smallest canonical schedule serialization — so the
    returned plan is a deterministic function of the instance."""
    if len(instance.actions) > max_actions:
        _refuse(
            REFUSE_INSTANCE_TOO_LARGE,
            f"the instance declares {len(instance.actions)} actions and the "
            f"declared oracle bound is {max_actions}; exhaustive "
            "enumeration is only claimed for tiny instances (§9.1)",
            actions=len(instance.actions), max_actions=max_actions)
    _require_known_quantities(instance)

    placement_sets = _enumerate_placements(instance, node_budget=node_budget)
    producer = producer_ref(max_actions=max_actions, node_budget=node_budget)

    best: tuple[Any, ...] | None = None
    best_result: OracleResult | None = None
    feasible = 0
    for placements in placement_sets:
        plan = _build_plan(instance, placements)
        certificate = build_certificate(instance, plan, producer=producer)
        result = check_schedule(instance, plan, certificate)
        if not result.accepted or result.recomputed_objective is None:
            continue
        feasible += 1
        key = objective_sort_key(result.recomputed_objective)
        if best is None or key < best:
            best = key
            best_result = OracleResult(
                plan=plan,
                certificate=certificate,
                check=result,
                best_objective=result.recomputed_objective,
                artifact=_artifact(
                    instance, result.recomputed_objective,
                    len(placement_sets), max_actions, node_budget),
                candidates_examined=len(placement_sets),
                feasible_candidates=0,
            )

    if best_result is None:
        _refuse(
            REFUSE_NO_FEASIBLE_CANDIDATE,
            "no enumerated candidate passed the independent checker; the "
            "instance has no publishable plan, which is an ANSWER, not a "
            "reason to relax a check",
            candidates=len(placement_sets))
        raise AssertionError("unreachable")

    # §12: the certificate that travels with an optimality claim BINDS
    # the artifact. Only now is there an artifact to bind, so the
    # winner's certificate is rebuilt around it; every certificate handed
    # out during the search stands behind no optimality claim and says so.
    return OracleResult(
        plan=best_result.plan,
        certificate=build_certificate(
            instance, best_result.plan, producer=producer,
            optimality_artifact_digest=best_result.artifact.digest),
        check=best_result.check,
        best_objective=best_result.best_objective,
        artifact=best_result.artifact,
        candidates_examined=len(placement_sets),
        feasible_candidates=feasible,
    )


def _artifact(
    instance: BatchInstance,
    best: ObjectiveTuple,
    candidates: int,
    max_actions: int,
    node_budget: int,
) -> OptimalityArtifact:
    return OptimalityArtifact(
        method=OPTIMALITY_METHOD_EXHAUSTIVE,
        instance_digest=instance.digest,
        unit_mapping_digest=instance.unit_mapping_digest,
        best_objective=best,
        candidates_examined=candidates,
        # Computed by the TRUSTED BASE, from the same canonical instance
        # the checker will use to recompute it. A digest only the
        # producer could evaluate would bind nothing.
        search_space_digest=search_space_digest(
            instance,
            method=OPTIMALITY_METHOD_EXHAUSTIVE,
            max_actions=max_actions,
            node_budget=node_budget,
            candidates_examined=candidates,
        ),
        producer_version=ORACLE_VERSION,
        max_actions=max_actions,
        node_budget=node_budget,
    )
