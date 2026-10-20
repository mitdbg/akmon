"""E2 frozen-batch scheduler comparison, plus E3's synthetic mechanics.

PACT ``concord-disclosure-budget``, task.batch.candidate-planner.
Normative sections: ``designs/batch-scheduling-research.md`` §8
(lexicographic objective), §9 and §9.1 (solvers are untrusted proposal
engines; "optimal" only through a checked artifact), §11 preamble
(preregistration), §11 E2, §11 E3, and §12 (reproducibility).

What this module is
-------------------
An experiment harness. It builds frozen batch instances, asks each
candidate planner for a plan, and hands every plan to the INDEPENDENT
checker. It never decides feasibility, value, or optimality itself:

* :func:`~...batch_schedule_checker.check_schedule` is the only validity
  and value authority; this module records its verdict and never
  re-derives, overrides, or softens it;
* :func:`~...batch_schedule_oracle.enumerate_optimum` and
  :func:`~...batch_schedule_checker.check_optimality_artifact` are the
  only optimality authority. A candidate is labelled optimal here ONLY
  when the checker returns ``certified_optimal`` against an oracle
  artifact bound by that candidate's own certificate. Everywhere else the
  reported sentence is §9.1's fallback: a checked feasible candidate with
  a checked value and an empirical gap.

Nothing in this module is trusted. Every planner below is a proposal
generator in the idiom of
:mod:`okg.substrate.consistency.batch_schedule_baselines`, and a plan it
emits is a claim until the checker accepts it. A planner that emits an
infeasible plan is not a defect in the harness: the result is a NAMED
refusal cause, and it is reported.

Named deferrals — never silent gaps
-----------------------------------
Parts of E2/E3 are NOT executed here. Each one is a row in the
preregistered manifest (:data:`DEFERRED`) and in the report, so that an
absence can never be read as a pass. The three that subtract from E2
itself are also named IN the derived verdict
(:data:`PARTIAL_E2_DEFERRALS`): a clear run of what did execute reads as
``pass_partial_e2:<deferred clauses>``, never as a bare ``pass``, because
E2 as declared did not run. The corpus's own shortfall against the
design's "6-12 actions" is a row too
(``deferred.corpus_action_range_6_to_12``): that band is unsatisfiable
under the oracle's declared six-action bound, and enumerability was kept.
The three largest deferrals:

* ``cpsat_ilp_candidates``. §9 permits the first experiment to implement
  "both, one, or neither" solver family. This repository has no CP-SAT or
  ILP dependency, and adding one is a reviewed dependency decision that
  has not been taken. The candidate set is therefore the deterministic
  baseline family, a seeded-random diagnostic, and — within its size
  guard — the exact oracle as the reference point. Solver evaluation is
  DEFERRED, not absent.
* ``history_window_batches``. E2's data clause also names at least five
  deterministic 22-update histories replayed in windows of 1, 4, and 8.
  That needs a corpus-to-instance binding that does not exist yet (it is
  database-backed work). The hand-built edge cases and the seeded
  synthetic grid are the data actually used here.
* ``e3_stakeholder_elicitation``. E3's rankings, post-hoc cardinal
  utilities, and the 24-unit decision slice require an authorized human
  domain owner. Elicited data is never fabricated. Only E3's synthetic
  MECHANICS run here: clone/split non-credit, tier-boundary perturbation,
  and selected-set stability.

Cardinal utilities are not scheduler inputs anywhere in this module —
there are none, and :func:`assert_no_cardinal_utility_inputs` asserts the
structural fact rather than trusting the reader.

Determinism
-----------
Every draw goes through :class:`~okg.bench.synth.prf.SynthPrf`. There is
no ``random`` and no wall clock in any digested content. Wall-times are
measured through an INJECTABLE clock and reported BESIDE the comparison,
never inside it: a preregistered comparison whose digest moved with the
machine's load would not be a replay.
"""
from __future__ import annotations

import hashlib
import inspect
import json
import time
from dataclasses import dataclass, replace
from fractions import Fraction
from typing import Any, Callable, Mapping, Sequence

from okg.bench.synth.prf import SynthPrf
from okg.substrate.consistency.batch_schedule import (
    ARTIFACT_UNBOUND,
    CONTRACT_SCHEMA_VERSION,
    OUTCOME_SUCCESS,
    PRIORITY_MAY,
    PRIORITY_MUST,
    PRIORITY_SHOULD,
    TERMINATION_EXHAUSTED,
    BatchInstance,
    BatchLoadRefused,
    ObjectiveTuple,
    ProducerRef,
    ScheduleCertificate,
    ScheduledAction,
    SchedulePlan,
    Unknown,
    load_batch_instance,
    object_digest,
    objective_sort_key,
)
from okg.substrate.consistency.batch_schedule_baselines import (
    REFUSE_UNPLACEABLE,
    BaselineRefused,
    plan_dependency_tier_greedy,
    plan_fifo,
    plan_marginal_coverage_greedy,
)

# The frozen baseline module is the reference implementation of the
# untrusted-proposal idiom: earliest-feasible placement, coarse own
# disclosure claim, canonical plan assembly. The E2 roster needs three
# more family members, and re-implementing placement here would make the
# new members differ from the existing ones for reasons that have nothing
# to do with their selection rules. That file must not be edited, so the
# new members import its internals and reuse them verbatim.
from okg.substrate.consistency.batch_schedule_baselines import (
    _action_of,
    _assemble,
    _dependency_order,
    _fits,
    _known_cost,
    _known_duration,
    _tier_of,
)
from okg.substrate.consistency.batch_schedule_checker import (
    CAUSE_MANDATORY_UNIT_UNCOVERED,
    CHECKER_VERSION,
    OptimalityVerdict,
    ScheduleCheckResult,
    check_optimality_artifact,
    check_schedule,
)
from okg.substrate.consistency.batch_schedule_oracle import (
    DEFAULT_MAX_ACTIONS,
    DEFAULT_NODE_BUDGET,
    OracleRefused,
    OracleResult,
    build_certificate,
    enumerate_optimum,
)
from okg.substrate.errors import SubstrateError

__all__ = [
    "CANDIDATE_PRODUCER_FAMILY",
    "CANDIDATE_PRODUCER_VERSION",
    "CLAUSES",
    "COMPARATOR_CANDIDATE",
    "DEFAULT_COMPARATOR_THRESHOLD",
    "DEFAULT_REPLICATE_SEEDS",
    "DEFAULT_SYNTHETIC_SEEDS",
    "DEFERRED",
    "DETERMINISTIC_CANDIDATES",
    "HAND_BUILT",
    "OPTIMALITY_CERTIFIED",
    "OPTIMALITY_NOT_OPTIMUM",
    "OPTIMALITY_UNAVAILABLE",
    "PARTIAL_E2_DEFERRALS",
    "PLAN_MUTATIONS",
    "PREREGISTERED_MANIFEST_DIGEST",
    "RANDOM_CANDIDATE",
    "REPLICATE_NONE",
    "STATUS_CHECKED_FEASIBLE",
    "STATUS_PLANNER_REFUSED",
    "STATUS_REFUSED",
    "SYNTHETIC_ACTION_SIZES",
    "VERDICT_PASS",
    "VERDICT_PASS_PARTIAL_E2",
    "ZERO_TOLERANCE",
    "Clock",
    "BatchExperimentError",
    "CandidateComparatorRow",
    "CandidateOutcome",
    "ComparatorRow",
    "E2Manifest",
    "E2Report",
    "FalsificationRow",
    "InstanceSpec",
    "OracleRow",
    "RobustnessRow",
    "TimingRow",
    "assert_no_cardinal_utility_inputs",
    "build_manifest",
    "clone_obligation",
    "hand_built_specs",
    "instance_specs",
    "plan_coverage_per_cost_greedy",
    "plan_seeded_random",
    "plan_tier_earliest_finish",
    "run_report",
    "split_obligation",
    "swap_tier_boundary",
    "synthetic_raw",
]


class BatchExperimentError(SubstrateError):
    """The harness refused. A mis-built experiment proves nothing."""


#: The producer identity written into every candidate certificate (§12).
CANDIDATE_PRODUCER_VERSION = "okg.bench.synth.batch-experiments/1"
CANDIDATE_PRODUCER_FAMILY = "untrusted-baseline-family"

COST_UNIT = "tokens"
UNIT_MAPPING_VERSION = "e2-unit-map/1"


# --------------------------------------------------------------------------- #
# The three E2 baselines the frozen module does not carry.                     #
# --------------------------------------------------------------------------- #
def _refuse_unplaceable(message: str, **extra: Any) -> None:
    raise BaselineRefused(
        f"batch schedule baseline refused [{REFUSE_UNPLACEABLE}]: {message}",
        data={"code": REFUSE_UNPLACEABLE, **extra},
    )


def _closure_units(
    instance: BatchInstance, action_id: str, priority: str
) -> set[str]:
    """The coverage units at ``priority`` reached by an action's closure."""
    units: set[str] = set()
    for member in _dependency_order(instance, [action_id]):
        for obligation_id in _action_of(instance, member).covers:
            obligation = instance.obligation_by_id(obligation_id)
            if obligation is not None and obligation.priority == priority:
                units.add(obligation.unit.key)
    return units


def plan_coverage_per_cost_greedy(instance: BatchInstance) -> SchedulePlan:
    """Tier-first coverage-per-measured-cost, WITHIN the active tier only.

    E2's fourth baseline. It works one priority tier at a time and never
    compares a gain in one tier against a gain in another — §8's
    lexicographic order forbids the trade, and a ratio rule is exactly
    where that trade would sneak in. Within the active tier it takes the
    action whose prerequisite closure maximizes (new distinct units at
    this tier) / (pinned cost of the not-yet-chosen closure), as an exact
    :class:`~fractions.Fraction` — no floating point, so the ordering is
    the same on every machine. A free closure with a positive gain ranks
    ahead of every priced one. Ties fall to larger gain, then lower cost,
    then the canonical action id.

    Coverage is counted in UNITS, so a cloned or split obligation adds no
    ratio and cannot pull an action in."""
    chosen: list[str] = []
    covered: set[str] = set()

    def closure_cost(action_id: str) -> int:
        return sum(
            _known_cost(_action_of(instance, member))
            for member in _dependency_order(instance, [action_id])
            if member not in chosen
        )

    for priority in (PRIORITY_MUST, PRIORITY_SHOULD, PRIORITY_MAY):
        while True:
            candidates: list[tuple[Any, ...]] = []
            for action in instance.actions:
                if action.id in chosen:
                    continue
                gain = _closure_units(instance, action.id, priority) - covered
                if not gain:
                    continue
                cost = closure_cost(action.id)
                # Group 0 is "free and useful"; nothing priced can beat it.
                free = 0 if cost == 0 else 1
                ratio = (Fraction(-len(gain), 1) if cost == 0
                         else Fraction(-len(gain), cost))
                candidates.append((free, ratio, -len(gain), cost, action.id))
            if not candidates:
                break
            best_id = min(candidates)[-1]
            for member in _dependency_order(instance, [best_id]):
                if member not in chosen:
                    chosen.append(member)
                for obligation_id in _action_of(instance, member).covers:
                    obligation = instance.obligation_by_id(obligation_id)
                    if obligation is not None:
                        covered.add(obligation.unit.key)

    return _assemble(instance, _dependency_order(instance, chosen))


def plan_tier_earliest_finish(instance: BatchInstance) -> SchedulePlan:
    """Tier-then-earliest-finish list scheduling. E2's fifth baseline.

    Classic list scheduling: repeatedly take the ready action (all
    prerequisites already placed) with the lowest key
    ``(tier, earliest feasible finish, earliest feasible start, id)`` and
    place it at that start. It selects everything — list scheduling is an
    ORDERING rule, not a selection rule — so it defers nothing and its
    interest is entirely in makespan under scarce capacity.

    The placement simulation uses the frozen module's own feasibility
    scan, so the order handed to the assembler reproduces exactly the
    starts computed here."""
    remaining = [a.id for a in instance.actions]
    placed: dict[str, Any] = {}
    ordered: list[str] = []
    horizon = sum(_known_duration(a) for a in instance.actions) + max(
        (s.start for r in instance.resources for s in r.segments), default=0
    ) + 1

    while remaining:
        best: tuple[Any, ...] | None = None
        blocked: list[str] = []
        for action_id in sorted(remaining):
            action = _action_of(instance, action_id)
            if any(p not in placed for p in action.requires):
                continue
            earliest = max(
                (placed[p].end for p in action.requires), default=0)
            start = earliest
            while (start <= horizon
                   and not _fits(instance, action, start, placed)):
                start += 1
            if start > horizon:
                blocked.append(action_id)
                continue
            finish = start + _known_duration(action)
            key = (_tier_of(instance, action), finish, start, action_id)
            if best is None or key < best:
                best = key
        if best is None:
            _refuse_unplaceable(
                "no ready action has a feasible start within the horizon; "
                "the remaining set is either cyclic or unschedulable",
                actions=",".join(sorted(remaining)),
                blocked=",".join(sorted(blocked)),
            )
            raise AssertionError("unreachable")
        chosen_id = best[-1]
        action = _action_of(instance, chosen_id)
        start = best[2]
        # Mirror the frozen assembler's ScheduledAction so the simulated
        # capacity state matches the one the assembler will rebuild.
        placed[chosen_id] = ScheduledAction(
            action_id=chosen_id,
            start=start,
            end=start + _known_duration(action),
            resource_assignments={},
            outcome=OUTCOME_SUCCESS,
        )
        ordered.append(chosen_id)
        remaining.remove(chosen_id)

    return _assemble(instance, tuple(ordered))


def _prf_dependency_order(
    instance: BatchInstance, wanted: Sequence[str], prf: SynthPrf, *path: str
) -> tuple[str, ...]:
    """A PRF-chosen topological order over the closure of ``wanted``."""
    needed: set[str] = set()
    frontier = list(wanted)
    while frontier:
        action_id = frontier.pop()
        if action_id in needed:
            continue
        needed.add(action_id)
        frontier.extend(_action_of(instance, action_id).requires)

    ordered: list[str] = []
    remaining = sorted(needed)
    while remaining:
        ready = [
            a for a in remaining
            if all(p in ordered for p in _action_of(instance, a).requires)
        ]
        if not ready:
            _refuse_unplaceable(
                "the prerequisite graph over the drawn set has a cycle",
                actions=",".join(remaining),
            )
            raise AssertionError("unreachable")
        pick = prf.choice(ready, *path, "order", str(len(ordered)))
        ordered.append(pick)
        remaining.remove(pick)
    return tuple(ordered)


def plan_seeded_random(instance: BatchInstance, *, seed: str) -> SchedulePlan:
    """The seeded random DIAGNOSTIC (E2), keyed by PRF — never ``random``.

    The draw is declared, because a diagnostic whose sampling rule is
    unstated measures nothing. Two rules:

    1. every action covering a known ``must`` obligation, plus its
       prerequisite closure, is always included. The random arm is a
       floor for the SELECTION and ORDERING heuristics, not a stress on
       the hard feasibility condition — the checker owns that, and the
       mutation clause exercises it deliberately;
    2. every other action is included with probability 1/2, keyed by
       ``(seed, instance id, action id)``, and the execution order is a
       PRF-drawn topological order.

    Same seed, same instance, byte-equal plan. It may still be refused —
    a drawn set can be unplaceable under conflicts — and that refusal is
    an answer, reported by name."""
    prf = SynthPrf(f"e2-random:{seed}")
    wanted: set[str] = set()
    for action in instance.actions:
        for obligation_id in action.covers:
            obligation = instance.obligation_by_id(obligation_id)
            if obligation is not None and obligation.priority == PRIORITY_MUST:
                wanted.add(action.id)
    for action in instance.actions:
        if action.id in wanted:
            continue
        if prf.uniform("select", instance.instance_id, action.id) < 0.5:
            wanted.add(action.id)
    order = _prf_dependency_order(
        instance, sorted(wanted), prf, "draw", instance.instance_id)
    return _assemble(instance, order)


#: The deterministic arms, in report order. ``fifo`` is the declared
#: simple-baseline comparator; the rest are the candidates measured
#: against it.
DETERMINISTIC_CANDIDATES: tuple[tuple[str, Callable[[BatchInstance],
                                                    SchedulePlan]], ...] = (
    ("fifo", plan_fifo),
    ("dependency_tier_greedy", plan_dependency_tier_greedy),
    ("marginal_coverage_greedy", plan_marginal_coverage_greedy),
    ("coverage_per_cost_greedy", plan_coverage_per_cost_greedy),
    ("tier_earliest_finish", plan_tier_earliest_finish),
)

COMPARATOR_CANDIDATE = "fifo"
RANDOM_CANDIDATE = "seeded_random"

#: The replicate label written on an arm that takes NO seed. Spelled as a
#: label rather than a number so a reader never mistakes a deterministic
#: row for replicate 0 of a sampled one.
REPLICATE_NONE = "-"


# --------------------------------------------------------------------------- #
# The instance corpus: hand-built edge cases plus a seeded synthetic grid.     #
# --------------------------------------------------------------------------- #
def _unit(name: str) -> dict[str, Any]:
    return {
        "spec_version": "e2-spec/1",
        "subject_scope_key": f"subject:{name}",
        "satisfaction_condition": f"condition:{name}",
    }


def _obligation(
    obligation_id: str, priority: str, unit_name: str,
    *, obligation_class: str = "generic",
) -> dict[str, Any]:
    return {
        "id": obligation_id,
        "obligation_class": obligation_class,
        "priority": priority,
        "unit": _unit(unit_name),
    }


def _action(
    action_id: str,
    *,
    demand: Mapping[str, Any],
    action_class: str = "check",
    covers: Sequence[str] = (),
    requires: Sequence[str] = (),
    conflicts: Sequence[str] = (),
    reveals: Any = None,
    effects: Any = None,
    duration: Any = 1,
    execution_cost: Any = 1,
) -> dict[str, Any]:
    """One action. ``demand`` names EVERY declared resource, explicitly.

    The loader refuses a silent omission (§7: an absent key is never a
    zero), and this corpus does not lean on the refusal — it writes the
    zeros."""
    return {
        "id": action_id,
        "action_class": action_class,
        "covers": list(covers),
        "requires": list(requires),
        "conflicts": list(conflicts),
        "reveals": (reveals if reveals is not None
                    else {"obligation_classes": [], "action_classes": []}),
        "effects": (effects if effects is not None
                    else {"locations": [f"loc:{action_id}"]}),
        "duration": duration,
        "execution_cost": execution_cost,
        "cost_unit": COST_UNIT,
        "demand": dict(demand),
        "estimator": {"id": "e2-pinned-estimator", "version": 1},
    }


def _instance(
    instance_id: str,
    *,
    obligations: Sequence[Mapping[str, Any]],
    actions: Sequence[Mapping[str, Any]],
    resources: Sequence[Mapping[str, Any]],
    conflict_groups: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    scope = sorted(
        {f"loc:{a['id']}" for a in actions} | {"loc:elsewhere"}
    )
    return {
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "instance_id": instance_id,
        "epoch": 0,
        "cost_unit": COST_UNIT,
        "unit_mapping_version": UNIT_MAPPING_VERSION,
        "changes": [{"id": "c1", "affected_scope": scope}],
        "obligations": [dict(o) for o in obligations],
        "actions": [dict(a) for a in actions],
        "conflict_groups": [dict(g) for g in conflict_groups],
        "resources": [dict(r) for r in resources],
    }


def _pool(
    resource_id: str,
    segments: Sequence[Mapping[str, Any]],
    *,
    reservation: Any = 1000,
    hard_cap: Any = 9,
) -> dict[str, Any]:
    return {
        "id": resource_id,
        "version": 1,
        "segments": [dict(s) for s in segments],
        "reservation": reservation,
        "hard_cap": hard_cap,
    }


@dataclass(frozen=True)
class InstanceSpec:
    """One frozen instance plus the reason it is in the corpus."""

    name: str
    raw: Mapping[str, Any]
    source: str
    #: What edge this instance exists to exercise. Reported, so a reader
    #: never has to reverse-engineer the corpus from its digests.
    exercises: str
    #: ``True`` for the instance deliberately built OVER the oracle's
    #: declared size guard, so the refusal is a documented row rather
    #: than a missing comparison.
    straddles_oracle_guard: bool = False

    def load(self) -> BatchInstance:
        return load_batch_instance(dict(self.raw))


def _spec_shared_coverage() -> InstanceSpec:
    """One shared action covering three units, plus a cheaper rival."""
    return InstanceSpec(
        name="shared-coverage",
        source="hand_built",
        exercises=(
            "one action covering many obligations; a rival action covering "
            "a unit the shared action already covers; an action covering "
            "nothing, which only a deferring candidate leaves out"
        ),
        raw=_instance(
            "e2-shared-coverage",
            obligations=[
                _obligation("o_gate", PRIORITY_MUST, "gate",
                            obligation_class="mandatory"),
                _obligation("o_x", PRIORITY_SHOULD, "x"),
                _obligation("o_y", PRIORITY_SHOULD, "y"),
                _obligation("o_w", PRIORITY_SHOULD, "w"),
                _obligation("o_z", PRIORITY_MAY, "z"),
            ],
            actions=[
                _action("a_gate", covers=["o_gate"], demand={"cpu": 1},
                        duration=1, execution_cost=2),
                _action("a_shared", action_class="sweep",
                        covers=["o_x", "o_y", "o_w"], requires=["a_gate"],
                        demand={"cpu": 1}, duration=2, execution_cost=4),
                _action("a_x_only", action_class="check", covers=["o_x"],
                        requires=["a_gate"], demand={"cpu": 1},
                        duration=1, execution_cost=3),
                _action("a_z", action_class="probe", covers=["o_z"],
                        demand={"cpu": 1}, duration=1, execution_cost=1),
                _action("a_idle", action_class="notice", demand={"cpu": 1},
                        duration=1, execution_cost=5),
            ],
            resources=[_pool("cpu", [{"start": 0, "capacity": 2}])],
        ),
    )


def _spec_prerequisite_closure() -> InstanceSpec:
    """Selecting a leaf pulls a chain that covers nothing itself."""
    return InstanceSpec(
        name="prerequisite-closure",
        source="hand_built",
        exercises=(
            "prerequisite closure is mandatory: the only action covering "
            "the `may` unit sits behind two actions that cover nothing, so "
            "its true price is the closure's price, not its own"
        ),
        raw=_instance(
            "e2-prerequisite-closure",
            obligations=[
                _obligation("o_must", PRIORITY_MUST, "must",
                            obligation_class="mandatory"),
                _obligation("o_should", PRIORITY_SHOULD, "should"),
                _obligation("o_may", PRIORITY_MAY, "may"),
            ],
            actions=[
                _action("a1", action_class="fetch", demand={"cpu": 1},
                        duration=1, execution_cost=2),
                _action("a2", action_class="index", requires=["a1"],
                        demand={"cpu": 1}, duration=1, execution_cost=2),
                _action("a3", action_class="probe", covers=["o_may"],
                        requires=["a2"], demand={"cpu": 1},
                        duration=1, execution_cost=1),
                _action("a4", action_class="gate", covers=["o_must"],
                        requires=["a1"], demand={"cpu": 1},
                        duration=2, execution_cost=3),
                _action("a5", action_class="repair", covers=["o_should"],
                        requires=["a4"], demand={"cpu": 1},
                        duration=1, execution_cost=2),
                _action("a6", action_class="notice", demand={"cpu": 1},
                        duration=1, execution_cost=6),
            ],
            resources=[_pool("cpu", [{"start": 0, "capacity": 2}])],
        ),
    )


def _spec_conflicts() -> InstanceSpec:
    """A pairwise conflict and a three-member conflict group."""
    return InstanceSpec(
        name="conflicts-and-group",
        source="hand_built",
        exercises=(
            "pairwise `conflicts` plus a bounded multi-action exclusion "
            "(a conflict group with max_concurrent 1), which no pairwise "
            "encoding expresses"
        ),
        raw=_instance(
            "e2-conflicts-and-group",
            obligations=[
                _obligation("o_must", PRIORITY_MUST, "must",
                            obligation_class="mandatory"),
                _obligation("o_s1", PRIORITY_SHOULD, "s1"),
                _obligation("o_s2", PRIORITY_SHOULD, "s2"),
                _obligation("o_m1", PRIORITY_MAY, "m1"),
            ],
            actions=[
                _action("a_root", action_class="fetch", covers=["o_must"],
                        demand={"cpu": 1}, duration=1, execution_cost=2),
                _action("a_p", action_class="write", covers=["o_s1"],
                        requires=["a_root"], conflicts=["a_q"],
                        demand={"cpu": 1}, duration=2, execution_cost=3),
                _action("a_q", action_class="write", covers=["o_s2"],
                        requires=["a_root"], demand={"cpu": 1},
                        duration=2, execution_cost=3),
                _action("a_r", action_class="scan", requires=["a_root"],
                        demand={"cpu": 1}, duration=1, execution_cost=1),
                _action("a_s", action_class="scan", requires=["a_root"],
                        demand={"cpu": 1}, duration=1, execution_cost=1),
                _action("a_t", action_class="scan", covers=["o_m1"],
                        requires=["a_root"], demand={"cpu": 1},
                        duration=1, execution_cost=1),
            ],
            conflict_groups=[{
                "id": "g_scan",
                "members": ["a_r", "a_s", "a_t"],
                "max_concurrent": 1,
            }],
            resources=[_pool("cpu", [{"start": 0, "capacity": 3}])],
        ),
    )


def _spec_cumulative_resources() -> InstanceSpec:
    """Two named pools with genuinely different scarcity."""
    return InstanceSpec(
        name="cumulative-resources",
        source="hand_built",
        exercises=(
            "two named cumulative pools: a wide cpu pool and a db pool of "
            "capacity 1 that serializes everything touching it, with every "
            "action naming BOTH resources explicitly"
        ),
        raw=_instance(
            "e2-cumulative-resources",
            obligations=[
                _obligation("o_must", PRIORITY_MUST, "must",
                            obligation_class="mandatory"),
                _obligation("o_s1", PRIORITY_SHOULD, "s1"),
                _obligation("o_s2", PRIORITY_SHOULD, "s2"),
                _obligation("o_m1", PRIORITY_MAY, "m1"),
            ],
            actions=[
                _action("a_root", action_class="fetch", covers=["o_must"],
                        demand={"cpu": 1, "db": 1}, duration=1,
                        execution_cost=2),
                _action("a_db1", action_class="query", covers=["o_s1"],
                        requires=["a_root"], demand={"cpu": 1, "db": 1},
                        duration=2, execution_cost=3),
                _action("a_db2", action_class="query", covers=["o_s2"],
                        requires=["a_root"], demand={"cpu": 1, "db": 1},
                        duration=2, execution_cost=3),
                _action("a_cpu1", action_class="compute", covers=["o_m1"],
                        requires=["a_root"], demand={"cpu": 2, "db": 0},
                        duration=1, execution_cost=1),
                _action("a_cpu2", action_class="compute",
                        requires=["a_root"], demand={"cpu": 2, "db": 0},
                        duration=1, execution_cost=4),
            ],
            resources=[
                _pool("cpu", [{"start": 0, "capacity": 3}]),
                _pool("db", [{"start": 0, "capacity": 1}], hard_cap=1),
            ],
        ),
    )


def _spec_capacity_breakpoint() -> InstanceSpec:
    """A capacity that widens partway through the epoch."""
    return InstanceSpec(
        name="capacity-breakpoint",
        source="hand_built",
        exercises=(
            "a versioned piecewise-constant capacity whose BREAKPOINT is a "
            "schedule event: capacity 1 until t=3, then 3. A planner that "
            "only ever looks at t=0 places everything on the narrow side"
        ),
        raw=_instance(
            "e2-capacity-breakpoint",
            obligations=[
                _obligation("o_must", PRIORITY_MUST, "must",
                            obligation_class="mandatory"),
                _obligation("o_s1", PRIORITY_SHOULD, "s1"),
                _obligation("o_s2", PRIORITY_SHOULD, "s2"),
                _obligation("o_m1", PRIORITY_MAY, "m1"),
            ],
            actions=[
                _action("a_root", action_class="fetch", covers=["o_must"],
                        demand={"cpu": 1}, duration=1, execution_cost=2),
                _action("a_w1", action_class="wide", covers=["o_s1"],
                        requires=["a_root"], demand={"cpu": 1},
                        duration=2, execution_cost=2),
                _action("a_w2", action_class="wide", covers=["o_s2"],
                        requires=["a_root"], demand={"cpu": 1},
                        duration=2, execution_cost=2),
                _action("a_w3", action_class="wide", covers=["o_m1"],
                        requires=["a_root"], demand={"cpu": 1},
                        duration=2, execution_cost=2),
            ],
            resources=[_pool(
                "cpu",
                [{"start": 0, "capacity": 1}, {"start": 3, "capacity": 3}],
            )],
        ),
    )


def _spec_must_critical_chain() -> InstanceSpec:
    """The mandatory unit sits at the end of a three-deep chain."""
    return InstanceSpec(
        name="must-critical-chain",
        source="hand_built",
        exercises=(
            "a must-critical prerequisite chain under capacity 1: the "
            "mandatory unit is discharged only by the last action of a "
            "three-deep chain, so a configuration that starves the chain "
            "has no publishable plan at all (§7.2)"
        ),
        raw=_instance(
            "e2-must-critical-chain",
            obligations=[
                _obligation("o_must", PRIORITY_MUST, "must",
                            obligation_class="mandatory"),
                _obligation("o_s1", PRIORITY_SHOULD, "s1"),
                _obligation("o_m1", PRIORITY_MAY, "m1"),
            ],
            actions=[
                _action("m1", action_class="stage", demand={"cpu": 1},
                        duration=1, execution_cost=1),
                _action("m2", action_class="stage", requires=["m1"],
                        demand={"cpu": 1}, duration=1, execution_cost=1),
                _action("m3", action_class="gate", covers=["o_must"],
                        requires=["m2"], demand={"cpu": 1},
                        duration=1, execution_cost=1),
                _action("s1", action_class="repair", covers=["o_s1"],
                        requires=["m1"], demand={"cpu": 1},
                        duration=2, execution_cost=5),
                _action("x1", action_class="probe", covers=["o_m1"],
                        requires=["m1"], demand={"cpu": 1},
                        duration=1, execution_cost=2),
                _action("x2", action_class="notice", requires=["m1"],
                        demand={"cpu": 1}, duration=1, execution_cost=7),
            ],
            resources=[_pool("cpu", [{"start": 0, "capacity": 1}])],
        ),
    )


def _spec_makespan_decides_parallel() -> InstanceSpec:
    """Every action is worth taking, so only the ORDER can differ.

    Built to make §8 stage 4 the DECIDING stage. Each action discharges a
    distinct unit, so no candidate can buy an earlier-stage win by
    deferring: coverage (stages 1-2) and pinned cost (stage 3) are equal
    for every arm that selects the whole set, and the comparison against
    FIFO is settled on MAKESPAN alone."""
    return InstanceSpec(
        name="makespan-decides-parallel",
        source="hand_built",
        exercises=(
            "makespan as the DECIDING objective stage: five actions, each "
            "covering a distinct unit, under a wide-but-finite pool. Every "
            "candidate selects everything, so stages 1-3 tie and only the "
            "placement order separates them — the one comparison the rest "
            "of the corpus cannot make"
        ),
        raw=_instance(
            "e2-makespan-decides-parallel",
            obligations=[
                _obligation("o_must", PRIORITY_MUST, "must",
                            obligation_class="mandatory"),
                _obligation("o_s1", PRIORITY_SHOULD, "s1"),
                _obligation("o_s2", PRIORITY_SHOULD, "s2"),
                _obligation("o_m1", PRIORITY_MAY, "m1"),
                _obligation("o_m2", PRIORITY_MAY, "m2"),
            ],
            actions=[
                _action("k_root", action_class="fetch", covers=["o_must"],
                        demand={"cpu": 1}, duration=1, execution_cost=1),
                _action("k_s1", action_class="check", covers=["o_s1"],
                        requires=["k_root"], demand={"cpu": 1},
                        duration=1, execution_cost=2),
                _action("k_s2", action_class="repair", covers=["o_s2"],
                        requires=["k_root"], demand={"cpu": 1},
                        duration=2, execution_cost=2),
                # The wide one: it needs two thirds of the pool at once, so
                # where FIFO's discovery order puts it decides the finish.
                _action("k_wide", action_class="compute", covers=["o_m1"],
                        requires=["k_root"], demand={"cpu": 2},
                        duration=1, execution_cost=2),
                _action("k_long", action_class="probe", covers=["o_m2"],
                        requires=["k_root"], demand={"cpu": 1},
                        duration=2, execution_cost=2),
            ],
            resources=[_pool("cpu", [{"start": 0, "capacity": 3}])],
        ),
    )


def _spec_makespan_decides_breakpoint() -> InstanceSpec:
    """The same stage-4 question, but the scarcity is a capacity EVENT.

    Capacity is 1 until t=2 and 2 afterwards. Every action covers a
    distinct unit, so stages 1-3 tie again; what separates the arms is
    whether the long action is started on the narrow side of the
    breakpoint or held for the wide side."""
    return InstanceSpec(
        name="makespan-decides-breakpoint",
        source="hand_built",
        exercises=(
            "makespan as the DECIDING stage under a piecewise-constant "
            "capacity: capacity 1 until t=2, then 2. Stages 1-3 tie across "
            "every candidate, so the comparison is settled by where each "
            "rule places the long action relative to the breakpoint"
        ),
        raw=_instance(
            "e2-makespan-decides-breakpoint",
            obligations=[
                _obligation("o_must", PRIORITY_MUST, "must",
                            obligation_class="mandatory"),
                _obligation("o_s1", PRIORITY_SHOULD, "s1"),
                _obligation("o_s2", PRIORITY_SHOULD, "s2"),
                _obligation("o_m1", PRIORITY_MAY, "m1"),
                _obligation("o_m2", PRIORITY_MAY, "m2"),
            ],
            actions=[
                _action("p_root", action_class="fetch", covers=["o_must"],
                        demand={"cpu": 1}, duration=1, execution_cost=1),
                _action("p_s1", action_class="check", covers=["o_s1"],
                        requires=["p_root"], demand={"cpu": 1},
                        duration=1, execution_cost=2),
                _action("p_s2", action_class="repair", covers=["o_s2"],
                        requires=["p_root"], demand={"cpu": 1},
                        duration=2, execution_cost=2),
                _action("p_wide", action_class="compute", covers=["o_m1"],
                        requires=["p_root"], demand={"cpu": 2},
                        duration=1, execution_cost=2),
                _action("p_long", action_class="probe", covers=["o_m2"],
                        requires=["p_root"], demand={"cpu": 1},
                        duration=3, execution_cost=2),
            ],
            resources=[_pool(
                "cpu",
                [{"start": 0, "capacity": 1}, {"start": 2, "capacity": 2}],
            )],
        ),
    )


def _spec_unenforceable_bound() -> InstanceSpec:
    """A resource whose reservation and cap are UNKNOWN."""
    return InstanceSpec(
        name="unenforceable-bound",
        source="hand_built",
        exercises=(
            "an UNKNOWN reservation and hard cap: the plan is still "
            "checkable, but §7 says an unknown bound cannot support a "
            "hard-budget guarantee, so the checker reports `not_established` "
            "and OPTIMALITY becomes unavailable even inside the oracle's "
            "size guard. Unknown is never read as zero"
        ),
        raw=_instance(
            "e2-unenforceable-bound",
            obligations=[
                _obligation("o_must", PRIORITY_MUST, "must",
                            obligation_class="mandatory"),
                _obligation("o_s1", PRIORITY_SHOULD, "s1"),
                _obligation("o_m1", PRIORITY_MAY, "m1"),
            ],
            actions=[
                _action("b_root", action_class="fetch", covers=["o_must"],
                        demand={"cpu": 1}, duration=1, execution_cost=2),
                _action("b_s", action_class="repair", covers=["o_s1"],
                        requires=["b_root"], demand={"cpu": 1},
                        duration=1, execution_cost=2),
                _action("b_m", action_class="probe", covers=["o_m1"],
                        requires=["b_root"], demand={"cpu": 1},
                        duration=1, execution_cost=1),
                _action("b_idle", action_class="notice",
                        requires=["b_root"], demand={"cpu": 1},
                        duration=1, execution_cost=4),
            ],
            resources=[_pool(
                "cpu", [{"start": 0, "capacity": 2}],
                reservation={"unknown": "the pool's budget was never measured"},
                hard_cap={"unknown": "no enforceable cap is configured"},
            )],
        ),
    )


def _spec_unknown_revelation() -> InstanceSpec:
    """Deferring an action with an UNKNOWN revelation refuses publication."""
    return InstanceSpec(
        name="unknown-revelation-defer",
        source="hand_built",
        exercises=(
            "§9's stricter rule: an UNKNOWN revelation on a DEFERRED action "
            "refuses qualified publication outright. A candidate that "
            "selects everything is unaffected; a deferring candidate is "
            "refused by name, which is the answer, not a harness failure"
        ),
        raw=_instance(
            "e2-unknown-revelation",
            obligations=[
                _obligation("o_must", PRIORITY_MUST, "must",
                            obligation_class="mandatory"),
                _obligation("o_s1", PRIORITY_SHOULD, "s1"),
            ],
            actions=[
                _action("r_root", action_class="fetch", covers=["o_must"],
                        demand={"cpu": 1}, duration=1, execution_cost=1),
                _action("r_s", action_class="repair", covers=["o_s1"],
                        requires=["r_root"], demand={"cpu": 1},
                        duration=1, execution_cost=1),
                _action("r_opaque", action_class="notice",
                        requires=["r_root"], demand={"cpu": 1},
                        duration=1, execution_cost=3,
                        reveals={"unknown": (
                            "this notice has never been run; what it may "
                            "surface is not bounded")}),
                _action("r_wide", action_class="notice",
                        requires=["r_root"], demand={"cpu": 1},
                        duration=1, execution_cost=3,
                        effects={"unknown": (
                            "the writer's result locations are unsummarized"
                        )}),
            ],
            resources=[_pool("cpu", [{"start": 0, "capacity": 2}])],
        ),
    )


def _spec_over_guard() -> InstanceSpec:
    """Eight actions: deliberately over the oracle's declared bound."""
    actions = [
        _action("g_root", action_class="fetch", covers=["o_must"],
                demand={"cpu": 1}, duration=1, execution_cost=2),
    ]
    for index in range(1, 8):
        covers = ["o_s1"] if index == 1 else ["o_m1"] if index == 2 else []
        actions.append(_action(
            f"g{index}", action_class="scan", covers=covers,
            requires=[f"g{index - 1}" if index > 1 else "g_root"],
            demand={"cpu": 1}, duration=1, execution_cost=index,
        ))
    return InstanceSpec(
        name="over-oracle-guard",
        source="hand_built",
        straddles_oracle_guard=True,
        exercises=(
            "eight actions, deliberately OVER the oracle's declared "
            "six-action bound. Every candidate is still checked; the "
            "oracle REFUSES `instance_too_large` and the refusal is a "
            "reported row, so no gap column is silently blank and no "
            "candidate here can be called optimal (§9.1's fallback)"
        ),
        raw=_instance(
            "e2-over-oracle-guard",
            obligations=[
                _obligation("o_must", PRIORITY_MUST, "must",
                            obligation_class="mandatory"),
                _obligation("o_s1", PRIORITY_SHOULD, "s1"),
                _obligation("o_m1", PRIORITY_MAY, "m1"),
            ],
            actions=actions,
            resources=[_pool("cpu", [{"start": 0, "capacity": 2}])],
        ),
    )


def hand_built_specs() -> tuple[InstanceSpec, ...]:
    """The hand-built E2 edge cases, in canonical order."""
    return (
        _spec_shared_coverage(),
        _spec_prerequisite_closure(),
        _spec_conflicts(),
        _spec_cumulative_resources(),
        _spec_capacity_breakpoint(),
        _spec_must_critical_chain(),
        _spec_makespan_decides_parallel(),
        _spec_makespan_decides_breakpoint(),
        _spec_unenforceable_bound(),
        _spec_unknown_revelation(),
        _spec_over_guard(),
    )


#: The names of the hand-built corpus, pinned into the manifest so an
#: instance cannot be quietly added or dropped between runs.
HAND_BUILT: tuple[str, ...] = tuple(s.name for s in hand_built_specs())

#: The DECLARED size distribution of the synthetic generator: the action
#: count is drawn uniformly from this tuple. It stays at or under the
#: oracle's six-action bound on purpose — a synthetic instance the oracle
#: cannot admit would add candidates without adding a reference.
SYNTHETIC_ACTION_SIZES: tuple[int, ...] = (4, 5)


def synthetic_raw(seed: int) -> dict[str, Any]:
    """One seeded synthetic instance. Every draw is PRF-keyed.

    Declared distribution: action count uniform over
    :data:`SYNTHETIC_ACTION_SIZES`; durations uniform in [1, 2]; costs
    uniform in [1, 6]; cpu demand uniform in [1, 2]; db demand 0 or 1;
    each action after the first requires the previous one with
    probability 1/2 (which keeps the graph acyclic BY CONSTRUCTION —
    an edge only ever points backwards); cpu capacity uniform in [1, 3]
    with a breakpoint at t=2 half the time. Obligations are attached to
    the first three actions as must / should / may so that every
    synthetic instance has a mandatory unit to discharge.

    The distribution does NOT guarantee feasibility, on purpose: a draw
    whose mandatory action demands more of a pool than the pool ever has
    admits no publishable plan at all. That instance is reported as an
    oracle ``no_feasible_candidate`` and a planner refusal on every arm —
    an answer — rather than being repaired away by narrowing the draw
    until every instance is solvable."""
    prf = SynthPrf(f"e2-synth:{seed}")
    count = prf.choice(SYNTHETIC_ACTION_SIZES, "size")
    capacity = prf.randint(1, 3, "capacity")
    segments: list[dict[str, Any]] = [{"start": 0, "capacity": capacity}]
    if prf.uniform("breakpoint") < 0.5:
        segments.append({"start": 2, "capacity": capacity + 2})

    obligations = [
        _obligation("o_must", PRIORITY_MUST, f"must-{seed}",
                    obligation_class="mandatory"),
        _obligation("o_should", PRIORITY_SHOULD, f"should-{seed}"),
        _obligation("o_may", PRIORITY_MAY, f"may-{seed}"),
    ]
    covers_by_index = {0: ["o_must"], 1: ["o_should"], 2: ["o_may"]}

    actions: list[dict[str, Any]] = []
    for index in range(count):
        action_id = f"y{index}"
        requires: list[str] = []
        if index > 0 and prf.uniform("edge", action_id) < 0.5:
            requires.append(f"y{index - 1}")
        actions.append(_action(
            action_id,
            action_class=prf.choice(("check", "repair", "probe"),
                                    "class", action_id),
            covers=covers_by_index.get(index, []),
            requires=requires,
            demand={
                "cpu": prf.randint(1, 2, "cpu", action_id),
                "db": prf.randint(0, 1, "db", action_id),
            },
            duration=prf.randint(1, 2, "duration", action_id),
            execution_cost=prf.randint(1, 6, "cost", action_id),
        ))
    return _instance(
        f"e2-synth-{seed}",
        obligations=obligations,
        actions=actions,
        resources=[
            _pool("cpu", segments),
            _pool("db", [{"start": 0, "capacity": 1}], hard_cap=1),
        ],
    )


def instance_specs(manifest: "E2Manifest") -> tuple[InstanceSpec, ...]:
    """The manifest's whole corpus: hand-built cases then the synthetic grid."""
    specs = [s for s in hand_built_specs() if s.name in manifest.hand_built]
    missing = set(manifest.hand_built) - {s.name for s in specs}
    if missing:
        raise BatchExperimentError(
            "the manifest names a hand-built instance the corpus does not "
            "define; a missing instance is never a silently smaller run",
            data={"missing": sorted(missing)},
        )
    for seed in manifest.synthetic_seeds:
        specs.append(InstanceSpec(
            name=f"synthetic-{seed}",
            source="synthetic",
            exercises=(
                "seeded synthetic instance from the declared size "
                "distribution; the grid measures whether the hand-built "
                "conclusions survive shapes nobody chose by hand"
            ),
            raw=synthetic_raw(seed),
        ))
    return tuple(specs)


# --------------------------------------------------------------------------- #
# E3 mechanics: clone, split, tier-boundary perturbation.                      #
# --------------------------------------------------------------------------- #
def clone_obligation(raw: Mapping[str, Any], obligation_id: str
                     ) -> dict[str, Any]:
    """Duplicate one obligation onto the SAME coverage unit.

    The clone keeps the original's unit and priority, and every action
    covering the original also covers the clone. §6 counts coverage in
    UNITS, so this must buy exactly nothing: an objective that improves
    here has been gamed by relabelling, which is E3's clone falsifier."""
    out = json.loads(json.dumps(dict(raw)))
    source = next(
        (o for o in out["obligations"] if o["id"] == obligation_id), None)
    if source is None:
        raise BatchExperimentError(
            "cannot clone an obligation the instance does not declare",
            data={"obligation": obligation_id},
        )
    clone = json.loads(json.dumps(source))
    clone["id"] = f"{obligation_id}#clone"
    out["obligations"].append(clone)
    for action in out["actions"]:
        if obligation_id in action["covers"]:
            action["covers"].append(clone["id"])
    out["instance_id"] = f"{out['instance_id']}+clone"
    return out


def split_obligation(raw: Mapping[str, Any], obligation_id: str
                     ) -> dict[str, Any]:
    """Split one obligation into two halves on the SAME coverage unit.

    The finding is re-filed as two obligations rather than one; the unit
    is unchanged, so the coverage value must be unchanged too. This is
    the other half of E3's clone/split sensitivity: reporting practice
    must not move the schedule."""
    out = json.loads(json.dumps(dict(raw)))
    source = next(
        (o for o in out["obligations"] if o["id"] == obligation_id), None)
    if source is None:
        raise BatchExperimentError(
            "cannot split an obligation the instance does not declare",
            data={"obligation": obligation_id},
        )
    halves = []
    for suffix in ("a", "b"):
        half = json.loads(json.dumps(source))
        half["id"] = f"{obligation_id}#{suffix}"
        halves.append(half)
    out["obligations"] = [
        o for o in out["obligations"] if o["id"] != obligation_id
    ] + halves
    for action in out["actions"]:
        if obligation_id in action["covers"]:
            action["covers"] = [
                c for c in action["covers"] if c != obligation_id
            ] + [h["id"] for h in halves]
    out["instance_id"] = f"{out['instance_id']}+split"
    return out


def swap_tier_boundary(raw: Mapping[str, Any]) -> dict[str, Any] | None:
    """Move the should/may boundary: one ``should`` and one ``may`` swap.

    E3 compares "the tier plan with alternative tier boundary
    placements". The perturbation NEVER touches ``must``: the mandatory
    set is the hard feasibility condition, and a perturbation that moved
    it would be testing a different question. Returns ``None`` when the
    instance has no should/may pair to swap — reported as not applicable,
    never as a pass."""
    out = json.loads(json.dumps(dict(raw)))
    should = next(
        (o for o in out["obligations"] if o["priority"] == PRIORITY_SHOULD),
        None)
    may = next(
        (o for o in out["obligations"] if o["priority"] == PRIORITY_MAY), None)
    if should is None or may is None:
        return None
    should["priority"] = PRIORITY_MAY
    may["priority"] = PRIORITY_SHOULD
    out["instance_id"] = f"{out['instance_id']}+tierswap"
    return out


def assert_no_cardinal_utility_inputs() -> None:
    """Structural assertion: no elicited cardinal value reaches a planner.

    E3 is explicit that cardinal values score robustness and never
    control the scheduler. In this slice there are no elicited values at
    all (the elicitation half is DEFERRED), so the assertion is that the
    batch contract itself has no place to put one: the objective's only
    numeric stages are a distinct-unit COUNT, a pinned operational COST,
    and a makespan. Priority is ordinal and lives on the coverage unit.

    Raises :class:`BatchExperimentError` if the objective ever grows a
    field this list does not name."""
    expected = {"should_units", "may_units", "total_cost", "makespan",
                "tie_break"}
    fields = set(ObjectiveTuple(0, 0, 0, 0, "").to_canonical_dict())
    if fields != expected:
        raise BatchExperimentError(
            "the lexicographic objective grew or lost a stage; a new "
            "numeric stage is exactly where an elicited cardinal utility "
            "would enter the scheduler, and §11 E3 forbids that",
            data={"fields": sorted(fields)},
        )


# --------------------------------------------------------------------------- #
# The preregistered manifest.                                                  #
# --------------------------------------------------------------------------- #
#: Generator seeds for the synthetic grid.
DEFAULT_SYNTHETIC_SEEDS: tuple[int, ...] = (101, 102, 103, 104)

#: Replicate seeds for the seeded-random diagnostic and for the
#: selected-set stability check. Three replicates: the deterministic arms
#: are exhaustive per instance, so seeds vary the DRAW, not sampling noise.
DEFAULT_REPLICATE_SEEDS: tuple[int, ...] = (7, 19, 31)

#: The comparator threshold, fixed BEFORE any outcome was inspected: a
#: candidate family earns the performance claim only if some non-FIFO
#: deterministic candidate strictly beats stable FIFO's checked tuple on
#: at least this fraction of the instances where both are checkable.
DEFAULT_COMPARATOR_THRESHOLD = Fraction(1, 2)

#: Every falsification clause E2 and E3's synthetic half declare, with
#: the text used to report it. A clause is reported whether it fires or
#: not — a falsifier nobody can see is not a falsifier.
CLAUSES: Mapping[str, str] = {
    "publishable_plan_with_unresolved_must": (
        "E2: any publishable plan with unresolved `must`. Evaluated over "
        "the checker's own verdicts: a candidate reported publishable "
        "whose findings include mandatory_unit_uncovered"
    ),
    "checker_accepts_mutated_plan": (
        "E2: any checker acceptance of a mutated dependency, conflict, "
        "capacity, or objective. Evaluated by MUTATING every accepted "
        "plan five named ways, re-certifying each mutant so the binding "
        "digests still match, and requiring a refusal every time"
    ),
    "nondeterministic_tie_break": (
        "E2: a nondeterministic tie-break. Evaluated by replanning every "
        "candidate twice and comparing plan digests byte for byte"
    ),
    "simple_baseline_comparator_not_beaten": (
        "E2: failure to beat the preregistered simple-baseline comparison "
        "AFTER PLANNING OVERHEAD [DEFERRED: overhead_adjusted_comparator — "
        "the comparison evaluated here is on the checked objective tuple "
        "ALONE; planning and checking wall-times are reported beside it "
        "and no overhead term enters the threshold] under the declared "
        "seeds, aggregation, uncertainty method, and threshold. "
        "Comparator: stable FIFO's checked lexicographic tuple. The "
        "clause is existential over the candidate pool; per-candidate "
        "beat-FIFO rows and the equivalence disclosure are reported "
        "beside it so a pool result is never read as a per-rule one"
    ),
    "clone_or_split_changes_coverage_value": (
        "E3: any objective improvement obtained merely by cloning (or "
        "splitting) an obligation. Evaluated for EVERY candidate over "
        "every clone and split variant of every instance, and evaluated "
        "over the WHOLE decision, not only the coverage stages: the "
        "checker's status, stages 1-4 of the objective (should units, "
        "may units, pinned total cost, pinned makespan) and the SELECTED "
        "SET must all be identical. Coverage units are invariant under "
        "both mutators by construction, so a check confined to stages "
        "1-2 could not observe the gaming it names — cost and makespan "
        "are where a relabelling-driven improvement lands"
    ),
    "tier_boundary_swap_changes_must_set": (
        "E3: any `must` disagreement under the preregistered "
        "classification rule. The perturbation swaps one should with one "
        "may and never touches must, so the mandatory unit set and every "
        "candidate's mandatory-coverage outcome must be unchanged"
    ),
    "deterministic_candidate_unstable_across_seeds": (
        "E3: selected-set instability. A deterministic candidate's "
        "selected set must be a function of the INSTANCE alone. Evaluated "
        "by re-planning every deterministic arm through the same "
        "seed-taking entry point under every replicate seed and requiring "
        "byte-equal plan digests. The entry point OFFERS the replicate "
        "seed to every arm that declares a `seed` parameter, so an arm "
        "that reads a seed and moves with it is caught; an arm that "
        "declares none has no channel to leak through, and for it the "
        "same probe measures dependence on global mutable state. The "
        "random arm is exempt: its across-seed spread is the "
        "measurement, not a defect"
    ),
}

#: The zero-tolerance subset (§11 preamble). Any one of these firing
#: refutes the slice; the comparator clause rejects a performance claim
#: without refuting the mechanism.
ZERO_TOLERANCE: tuple[str, ...] = (
    "publishable_plan_with_unresolved_must",
    "checker_accepts_mutated_plan",
    "nondeterministic_tie_break",
    "clone_or_split_changes_coverage_value",
    "tier_boundary_swap_changes_must_set",
    "deterministic_candidate_unstable_across_seeds",
)

#: The named deferrals. Each one is carried in the manifest and echoed
#: in the report: a deferral is a row, never a silent cap.
DEFERRED: Mapping[str, str] = {
    "cpsat_ilp_candidates": (
        "§9 permits neither solver family in the first experiment, and no "
        "CP-SAT or ILP dependency exists in this repository. Adding one is "
        "a reviewed dependency decision that has not been taken. Solver "
        "candidates are DEFERRED — the comparison below runs the "
        "deterministic baseline family, a seeded-random diagnostic, and "
        "the exact oracle as the reference, and every one of them is an "
        "untrusted proposal generator whose plans the checker validates"
    ),
    "history_window_batches": (
        "E2's data clause also names five deterministic 22-update "
        "histories replayed in windows of 1, 4, and 8. That needs the "
        "database-backed corpus-to-instance binding, which does not exist "
        "yet. DEFERRED; the hand-built edge cases and the seeded synthetic "
        "grid are the data actually used"
    ),
    "e3_stakeholder_elicitation": (
        "E3's independently elicited rankings, post-hoc cardinal "
        "utilities, and the frozen 24-unit / 8-per-tier decision slice "
        "require an AUTHORIZED HUMAN DOMAIN OWNER. Elicited data is never "
        "fabricated. DEFERRED; only E3's synthetic mechanics run here"
    ),
    "overhead_adjusted_comparator": (
        "E2's comparator says 'after planning overhead'. Wall-time is not "
        "reproducible, so a time-threshold inside a digested comparison "
        "would break exact replay. The comparator here is evaluated on "
        "the checked objective tuple, and planning/checking wall-times are "
        "reported BESIDE it. An overhead-adjusted threshold is DEFERRED "
        "until a pinned machine configuration (§12) is bound to the run"
    ),
    "realized_execution_measurements": (
        "§7.1 separates estimated, reserved, and REALIZED quantities. "
        "Nothing is executed here, so realized cost and duration are "
        "unavailable — they are reported as unavailable, never as the "
        "estimate and never as zero. The realized half belongs to the "
        "measurement adapter's live path"
    ),
    "corpus_action_range_6_to_12": (
        "E2's data clause asks for hand-built 6-12-action edge cases AND, "
        "in the next sentence, that the hand-built subset be bounded "
        "tightly enough for EXHAUSTIVE ENUMERATION. The oracle's declared "
        "bound is six actions (batch_schedule_oracle.DEFAULT_MAX_ACTIONS), "
        "so the 7-12-action band is jointly unsatisfiable with "
        "enumerability: an instance there has no oracle reference at all. "
        "Enumerability was kept, so the corpus runs 4-6 actions plus ONE "
        "deliberate 8-action instance (`over-oracle-guard`) whose oracle "
        "refusal is a reported row. The 7-12-action range is DEFERRED "
        "rather than silently dropped; the design-text contradiction is "
        "recorded in pact/changes/concord-disclosure-budget/reports/"
        "ewd-review-batch-experiments.md (EWD, 'On the design's 6-12 "
        "actions')"
    ),
    "certificate_size_metric": (
        "E2 lists certificate size among its metrics. The certificate is "
        "a canonical object whose size is a property of the encoding, not "
        "of the candidate; reporting it as a scheduler metric would "
        "mislead. DEFERRED pending a wire format worth measuring"
    ),
}


@dataclass(frozen=True)
class E2Manifest:
    """The preregistered E2/E3-mechanics manifest, frozen before outcomes.

    Built by :func:`build_manifest` from CONFIGURATION ONLY. It takes no
    result object of any kind, so a manifest cannot be shaped by what the
    run produced; :meth:`digest` binds every report row."""

    hand_built: tuple[str, ...]
    synthetic_seeds: tuple[int, ...]
    synthetic_action_sizes: tuple[int, ...]
    replicate_seeds: tuple[int, ...]
    candidates: tuple[str, ...]
    oracle_max_actions: int
    oracle_node_budget: int
    zero_tolerance: Mapping[str, str]
    falsification_clauses: Mapping[str, str]
    primary_metric: str
    comparator: str
    comparator_threshold: str
    aggregation: str
    uncertainty: str
    threshold: str
    solver_families: str
    deferred: Mapping[str, str]
    schema: str = "okg.synth.e2-batch-scheduler.v1"

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "hand_built": list(self.hand_built),
            "synthetic_seeds": list(self.synthetic_seeds),
            "synthetic_action_sizes": list(self.synthetic_action_sizes),
            "replicate_seeds": list(self.replicate_seeds),
            "candidates": list(self.candidates),
            "oracle_max_actions": self.oracle_max_actions,
            "oracle_node_budget": self.oracle_node_budget,
            "zero_tolerance": dict(sorted(self.zero_tolerance.items())),
            "falsification_clauses": dict(
                sorted(self.falsification_clauses.items())),
            "primary_metric": self.primary_metric,
            "comparator": self.comparator,
            "comparator_threshold": self.comparator_threshold,
            "aggregation": self.aggregation,
            "uncertainty": self.uncertainty,
            "threshold": self.threshold,
            "solver_families": self.solver_families,
            "deferred": dict(sorted(self.deferred.items())),
        }

    def canonical_json(self) -> str:
        return json.dumps(
            self.to_canonical_dict(), sort_keys=True, separators=(",", ":")
        )

    def digest(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


def build_manifest(
    *,
    hand_built: Sequence[str] = HAND_BUILT,
    synthetic_seeds: Sequence[int] = DEFAULT_SYNTHETIC_SEEDS,
    replicate_seeds: Sequence[int] = DEFAULT_REPLICATE_SEEDS,
    comparator_threshold: Fraction = DEFAULT_COMPARATOR_THRESHOLD,
) -> E2Manifest:
    """The one manifest constructor: configuration in, preregistration out."""
    if not hand_built:
        raise BatchExperimentError(
            "E2 needs its hand-built edge cases; a run over synthetic "
            "instances alone tests no edge anybody chose",
            data={"hand_built": list(hand_built)},
        )
    if not replicate_seeds:
        raise BatchExperimentError(
            "the seeded-random diagnostic and the stability check both need "
            "at least one replicate seed",
            data={"replicate_seeds": list(replicate_seeds)},
        )
    return E2Manifest(
        hand_built=tuple(hand_built),
        synthetic_seeds=tuple(int(s) for s in synthetic_seeds),
        synthetic_action_sizes=SYNTHETIC_ACTION_SIZES,
        replicate_seeds=tuple(int(s) for s in replicate_seeds),
        candidates=tuple(
            [name for name, _ in DETERMINISTIC_CANDIDATES] + [RANDOM_CANDIDATE]
        ),
        oracle_max_actions=DEFAULT_MAX_ACTIONS,
        oracle_node_budget=DEFAULT_NODE_BUDGET,
        zero_tolerance={
            name: CLAUSES[name] for name in ZERO_TOLERANCE
        },
        falsification_clauses=dict(CLAUSES),
        primary_metric=(
            "the §8 lexicographic objective tuple as RECOMPUTED by the "
            "independent checker: (distinct should units desc, distinct may "
            "units desc, pinned total cost asc, pinned makespan asc, "
            "canonical serialization asc). The planner's claimed value is "
            "never the metric"
        ),
        comparator=(
            "stable FIFO/discovery order (`fifo`), the simple baseline that "
            "does no selection at all. A candidate 'beats' it on an "
            "instance when the checker's recomputed tuple sorts strictly "
            "ahead of FIFO's under objective_sort_key"
        ),
        comparator_threshold=(
            f"{comparator_threshold.numerator}/"
            f"{comparator_threshold.denominator}"
        ),
        aggregation=(
            "per-instance exact comparison over the whole corpus; across "
            "replicate seeds the deterministic arms must be identical and "
            "the random arm is reported per seed plus min/max spread"
        ),
        uncertainty=(
            "deterministic exhaustive counts — no sampling and no "
            "confidence interval. Across-seed variation is reported as "
            "min/max spread. Oracle gaps are exact where the size guard "
            "admits the instance and REFUSED by name where it does not"
        ),
        threshold=(
            "every zero-tolerance clause at zero, AND some non-FIFO "
            "deterministic candidate strictly beating FIFO's checked tuple "
            f"on at least {comparator_threshold.numerator}/"
            f"{comparator_threshold.denominator} of the instances where "
            "both are checkable"
        ),
        solver_families=(
            "none. CP-SAT and ILP are DEFERRED (see deferred."
            "cpsat_ilp_candidates); the candidate set is the deterministic "
            "baseline family plus a seeded-random diagnostic, with the "
            "exhaustive oracle as the reference inside its size guard"
        ),
        deferred=dict(DEFERRED),
    )


#: The deferrals that make this run PARTIAL E2 rather than E2. Each one
#: subtracts from the experiment E2 declares — a proposal engine, a data
#: clause, or a term of the comparator itself — so a clear run of what
#: DID execute is not the same sentence as "E2 passed". The derived
#: verdict names them, because the module's own standard ("a deferral is
#: a row, never a silent cap") has to apply to the verdict string too.
PARTIAL_E2_DEFERRALS: tuple[str, ...] = (
    "cpsat_ilp_candidates",
    "history_window_batches",
    "overhead_adjusted_comparator",
)

#: The verdict for a clear run with NOTHING of E2 deferred.
VERDICT_PASS = "pass"
#: The verdict for a clear run of a PARTIAL E2. Always carries the
#: deferred clause names, so the partiality travels with the word.
VERDICT_PASS_PARTIAL_E2 = "pass_partial_e2"

#: The PREREGISTERED manifest digest: the digest of ``build_manifest()``
#: under its default configuration, pinned so that an outcome-driven edit
#: to the corpus, seeds, clauses, comparator, or threshold cannot pass
#: silently — changing the configuration means changing this constant in
#: the same reviewable diff. The witness asserts the equality.
PREREGISTERED_MANIFEST_DIGEST = (
    "45d76499412f9bf89157c6c571701a26e194d3d67ee13c39012323c8b383c93f"
)


# --------------------------------------------------------------------------- #
# Running one candidate against the checker.                                   #
# --------------------------------------------------------------------------- #
#: A clock returns integer nanoseconds. Injectable so a test can make the
#: measurement deterministic; the default is monotonic.
Clock = Callable[[], int]

STATUS_CHECKED_FEASIBLE = "checked_feasible"
STATUS_REFUSED = "refused"
STATUS_PLANNER_REFUSED = "planner_refused"

OPTIMALITY_CERTIFIED = "certified_optimal"
OPTIMALITY_NOT_OPTIMUM = "not_at_oracle_optimum"
OPTIMALITY_UNAVAILABLE = "unavailable"


def _candidate_producer(candidate: str, seed: str | None) -> ProducerRef:
    """§12 producer identity for a candidate plan.

    ``seed`` is UNKNOWN with a reason for the deterministic arms rather
    than a fictional ``0``; the random arm writes the seed it drew on."""
    return ProducerRef(
        family=CANDIDATE_PRODUCER_FAMILY,
        version=CANDIDATE_PRODUCER_VERSION,
        parameters_digest=object_digest(
            {"candidate": candidate, "seed": seed if seed is not None else ""}),
        seed=(int(seed) if seed is not None and seed.isdigit()
              else Unknown(
                  "this candidate is deterministic and draws no random value"
                  if seed is None else f"non-numeric replicate key {seed!r}")),
        termination_status=TERMINATION_EXHAUSTED,
    )


@dataclass(frozen=True)
class CandidateOutcome:
    """One (instance, candidate) row. Every field comes from the checker."""

    instance: str
    candidate: str
    replicate: str
    status: str
    causes: tuple[str, ...]
    objective: ObjectiveTuple | None
    not_established: tuple[str, ...]
    selected: tuple[str, ...]
    deferred: tuple[str, ...]
    plan_digest: str
    certificate_digest: str
    optimality: str
    optimality_detail: str
    #: Per-stage distance to the oracle optimum, signed (see
    #: :func:`_gap`: positive is worse, and a negative later stage is
    #: normal under a lexicographic optimum), plus ``first_worse_stage``.
    #: ``None`` where the oracle refused the instance — a blank column is
    #: never a zero gap.
    oracle_gap: Mapping[str, int] | None
    mutation_survivals: tuple[str, ...]
    #: How many of the five named mutations actually had somewhere to
    #: bite on this plan. A mutation with nowhere to apply is not a
    #: mutation that was caught.
    mutations_applied: int = 0

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "instance": self.instance,
            "candidate": self.candidate,
            "replicate": self.replicate,
            "status": self.status,
            "causes": list(self.causes),
            "objective": (None if self.objective is None
                          else self.objective.to_canonical_dict()),
            "not_established": list(self.not_established),
            "selected": list(self.selected),
            "deferred": list(self.deferred),
            "plan_digest": self.plan_digest,
            "certificate_digest": self.certificate_digest,
            "optimality": self.optimality,
            "optimality_detail": self.optimality_detail,
            "oracle_gap": (None if self.oracle_gap is None
                           else dict(sorted(self.oracle_gap.items()))),
            "mutation_survivals": list(self.mutation_survivals),
            "mutations_applied": self.mutations_applied,
        }


@dataclass(frozen=True)
class TimingRow:
    """Realized wall-times. Reported BESIDE the comparison, never in it."""

    instance: str
    candidate: str
    replicate: str
    planning_ns: int
    checking_ns: int


@dataclass(frozen=True)
class OracleRow:
    """Whether the oracle admitted an instance, and what it found."""

    instance: str
    admitted: bool
    refusal_code: str
    best_objective: ObjectiveTuple | None
    candidates_examined: int | None
    feasible_candidates: int | None
    note: str

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "instance": self.instance,
            "admitted": self.admitted,
            "refusal_code": self.refusal_code,
            "best_objective": (None if self.best_objective is None
                               else self.best_objective.to_canonical_dict()),
            "candidates_examined": self.candidates_examined,
            "feasible_candidates": self.feasible_candidates,
            "note": self.note,
        }


@dataclass(frozen=True)
class RobustnessRow:
    """One E3 synthetic-mechanics observation."""

    kind: str
    instance: str
    candidate: str
    detail: str
    invariant_held: bool

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "instance": self.instance,
            "candidate": self.candidate,
            "detail": self.detail,
            "invariant_held": self.invariant_held,
        }


@dataclass(frozen=True)
class FalsificationRow:
    """One clause, reported whether it fired or not."""

    clause: str
    fired: bool
    zero_tolerance: bool
    evaluated: int
    detail: str

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "clause": self.clause,
            "fired": self.fired,
            "zero_tolerance": self.zero_tolerance,
            "evaluated": self.evaluated,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class CandidateComparatorRow:
    """ONE candidate's own record against the comparator baseline.

    The preregistered clause is existential over the pool — "SOME
    non-FIFO deterministic candidate strictly beating FIFO" — and stays
    that way. This row is what the aggregate cannot say: which rule
    actually did it, and how the others fared. Without it a pool
    headline reads as a claim about the family when it may be a claim
    about one member."""

    candidate: str
    better: int
    tied: int
    worse: int
    not_comparable: int
    #: This candidate's OWN beat-FIFO fraction over the instances where
    #: it and FIFO are both checkable. Reported beside the preregistered
    #: pool fraction, never in place of it.
    beats_comparator: str

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "candidate": self.candidate,
            "better": self.better,
            "tied": self.tied,
            "worse": self.worse,
            "not_comparable": self.not_comparable,
            "beats_comparator": self.beats_comparator,
        }


@dataclass(frozen=True)
class ComparatorRow:
    """The preregistered simple-baseline comparison, computed not authored."""

    instances_compared: int
    instances_candidate_better: int
    instances_tied: int
    instances_comparator_better: int
    threshold: str
    achieved: str
    met: bool
    per_instance_winner: Mapping[str, str]
    #: Every candidate's own row (:class:`CandidateComparatorRow`).
    per_candidate: tuple[CandidateComparatorRow, ...] = ()
    #: Groups of candidates whose CHECKED OUTCOME is identical on every
    #: instance. Two members of one group are one rule implemented twice,
    #: and counting them as two independent winners would be a double
    #: count. Disclosed as a named row so the headline cannot read that way.
    equivalent_candidates: tuple[tuple[str, ...], ...] = ()
    #: Which §8 stage decided each comparison against the baseline
    #: (``stage_1_should_units`` .. ``stage_5_tie_break``, plus
    #: ``stage_0_identical``). A stage that never decides anything is a
    #: stage this corpus does not test, and the histogram says so.
    deciding_stages: Mapping[str, int] = ()  # type: ignore[assignment]
    #: The derived sentence a reader should quote. Computed from the rows
    #: above, never authored.
    headline: str = ""

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "instances_compared": self.instances_compared,
            "instances_candidate_better": self.instances_candidate_better,
            "instances_tied": self.instances_tied,
            "instances_comparator_better": self.instances_comparator_better,
            "threshold": self.threshold,
            "achieved": self.achieved,
            "met": self.met,
            "per_instance_winner": dict(sorted(self.per_instance_winner.items())),
            "per_candidate": [
                r.to_canonical_dict() for r in self.per_candidate],
            "equivalent_candidates": [
                list(group) for group in self.equivalent_candidates],
            "deciding_stages": dict(sorted(dict(self.deciding_stages).items())),
            "headline": self.headline,
        }


def _certify(
    instance: BatchInstance,
    plan: SchedulePlan,
    candidate: str,
    seed: str | None,
    *,
    artifact_digest: str = ARTIFACT_UNBOUND,
) -> ScheduleCertificate:
    return build_certificate(
        instance, plan,
        producer=_candidate_producer(candidate, seed),
        optimality_artifact_digest=artifact_digest,
    )


#: Cache for :func:`_takes_seed`. Keyed by the planner object, so a
#: test-local roster is answered for itself and never for the shipped one.
_SEED_TAKING: dict[Any, bool] = {}


def _takes_seed(planner: Callable[..., SchedulePlan]) -> bool:
    """Does this planner DECLARE a ``seed`` parameter?

    The seed-leak probe is only a probe if the seed can actually reach
    the arm under test. Every planner is offered the replicate seed
    through :func:`_plan_candidate`; one that declares a ``seed``
    parameter receives it, so an arm that reads a seed and varies with it
    is caught by the across-seed digest comparison. An arm that declares
    no seed parameter has no channel to leak through and is offered
    none — for it, the same probe measures dependence on GLOBAL mutable
    state, which is the other way a 'deterministic' arm can move."""
    cached = _SEED_TAKING.get(planner)
    if cached is None:
        cached = "seed" in inspect.signature(planner).parameters
        _SEED_TAKING[planner] = cached
    return cached


def _plan_candidate(
    instance: BatchInstance, candidate: str, replicate: str
) -> SchedulePlan:
    """The ONE seed-taking entry point every candidate is planned through."""
    for name, planner in DETERMINISTIC_CANDIDATES:
        if name == candidate:
            if _takes_seed(planner):
                return planner(instance, seed=replicate)
            return planner(instance)
    if candidate == RANDOM_CANDIDATE:
        return plan_seeded_random(instance, seed=replicate)
    raise BatchExperimentError(
        "unknown candidate; the roster is closed",
        data={"candidate": candidate},
    )


# The five named plan mutations for the "checker accepts a mutated plan"
# clause. Each returns a mutated plan or None when the accepted plan has
# no place to apply it; a mutation that cannot apply is reported as not
# evaluated rather than as a pass.
def _mutate_drop_prerequisite(
    instance: BatchInstance, plan: SchedulePlan
) -> SchedulePlan | None:
    selected = {s.action_id for s in plan.selected}
    for action in instance.actions:
        if action.id not in selected:
            continue
        for prerequisite in action.requires:
            if prerequisite in selected:
                return SchedulePlan(
                    schema_version=plan.schema_version,
                    instance_id=plan.instance_id,
                    selected=tuple(
                        s for s in plan.selected
                        if s.action_id != prerequisite),
                    deferred=tuple(sorted(plan.deferred + (prerequisite,))),
                    witnesses=plan.witnesses,
                    may_change=plan.may_change,
                    claimed_objective=plan.claimed_objective,
                )
    return None


def _mutate_break_temporal_order(
    instance: BatchInstance, plan: SchedulePlan
) -> SchedulePlan | None:
    by_id = {s.action_id: s for s in plan.selected}
    for action in instance.actions:
        scheduled = by_id.get(action.id)
        if scheduled is None or not action.requires:
            continue
        if scheduled.start == 0:
            continue
        moved = replace(
            scheduled, start=0,
            end=scheduled.end - scheduled.start)
        return SchedulePlan(
            schema_version=plan.schema_version,
            instance_id=plan.instance_id,
            selected=tuple(
                moved if s.action_id == action.id else s
                for s in plan.selected),
            deferred=plan.deferred,
            witnesses=plan.witnesses,
            may_change=plan.may_change,
            claimed_objective=plan.claimed_objective,
        )
    return None


def _mutate_stretch_duration(
    instance: BatchInstance, plan: SchedulePlan
) -> SchedulePlan | None:
    if not plan.selected:
        return None
    first = sorted(plan.selected, key=lambda s: s.action_id)[0]
    stretched = replace(first, end=first.end + 1)
    return SchedulePlan(
        schema_version=plan.schema_version,
        instance_id=plan.instance_id,
        selected=tuple(
            stretched if s.action_id == first.action_id else s
            for s in plan.selected),
        deferred=plan.deferred,
        witnesses=plan.witnesses,
        may_change=plan.may_change,
        claimed_objective=plan.claimed_objective,
    )


def _mutate_inflate_objective(
    instance: BatchInstance, plan: SchedulePlan
) -> SchedulePlan | None:
    return SchedulePlan(
        schema_version=plan.schema_version,
        instance_id=plan.instance_id,
        selected=plan.selected,
        deferred=plan.deferred,
        witnesses=plan.witnesses,
        may_change=plan.may_change,
        claimed_objective=replace(
            plan.claimed_objective,
            should_units=plan.claimed_objective.should_units + 1),
    )


def _mutate_shrink_disclosure(
    instance: BatchInstance, plan: SchedulePlan
) -> SchedulePlan | None:
    if not plan.may_change:
        return None
    return SchedulePlan(
        schema_version=plan.schema_version,
        instance_id=plan.instance_id,
        selected=plan.selected,
        deferred=plan.deferred,
        witnesses=plan.witnesses,
        may_change=tuple(sorted(plan.may_change)[1:]),
        claimed_objective=plan.claimed_objective,
    )


PLAN_MUTATIONS: tuple[tuple[str, Callable[..., SchedulePlan | None]], ...] = (
    ("drop_prerequisite_from_selection", _mutate_drop_prerequisite),
    ("break_temporal_order", _mutate_break_temporal_order),
    ("stretch_duration", _mutate_stretch_duration),
    ("inflate_claimed_objective", _mutate_inflate_objective),
    ("shrink_disclosure_set", _mutate_shrink_disclosure),
)


def _mutation_survivals(
    instance: BatchInstance, plan: SchedulePlan, candidate: str, seed: str | None
) -> tuple[tuple[str, ...], int]:
    """Mutations the checker ACCEPTED, and how many were APPLICABLE.

    Every mutant is re-certified so its binding digests match; otherwise
    the refusal would be a digest mismatch and the constraint check would
    never be reached. A mutation that has nowhere to bite on this plan —
    there is no deferred action to un-defer, no disclosure location to
    drop — is not applied, and is not counted as a mutation that was
    caught. Counting it would inflate a 'clear' result with evaluations
    that never happened."""
    survived: list[str] = []
    applied = 0
    for name, mutate in PLAN_MUTATIONS:
        mutant = mutate(instance, plan)
        if mutant is None:
            continue
        applied += 1
        certificate = _certify(instance, mutant, candidate, seed)
        if check_schedule(instance, mutant, certificate).accepted:
            survived.append(name)
    return tuple(survived), applied


def _gap(best: ObjectiveTuple, value: ObjectiveTuple) -> dict[str, int]:
    """Per-stage distance from the oracle optimum.

    Sign convention: POSITIVE means the candidate is worse than the
    optimum at that stage. A NEGATIVE entry is possible and is not a
    contradiction — the optimum dominates LEXICOGRAPHICALLY, not stage by
    stage, so a candidate that gives up a `should` unit may well come in
    under the optimum's cost. Reading one stage on its own is therefore a
    mistake, which is why ``first_worse_stage`` is carried beside them:
    it names the stage that actually decided the comparison (1..4, or 0
    when the candidate's tuple equals the optimum at every stage)."""
    stages = [
        ("should_units", best.should_units - value.should_units),
        ("may_units", best.may_units - value.may_units),
        ("total_cost", value.total_cost - best.total_cost),
        ("makespan", value.makespan - best.makespan),
    ]
    first_worse = 0
    for index, (_, delta) in enumerate(stages, start=1):
        if delta != 0:
            first_worse = index
            break
    gap = dict(stages)
    gap["first_worse_stage"] = first_worse
    return gap


def _run_candidate(
    spec: InstanceSpec,
    instance: BatchInstance,
    candidate: str,
    replicate: str,
    oracle: OracleResult | None,
    oracle_row: OracleRow,
    clock: Clock,
) -> tuple[CandidateOutcome, TimingRow]:
    seed = replicate if candidate == RANDOM_CANDIDATE else None

    start = clock()
    try:
        plan = _plan_candidate(instance, candidate, replicate)
    except BaselineRefused as refusal:
        planning_ns = clock() - start
        code = str(refusal.data.get("code", "unnamed"))
        return (
            CandidateOutcome(
                instance=spec.name, candidate=candidate, replicate=replicate,
                status=STATUS_PLANNER_REFUSED,
                causes=(f"planner:{code}",),
                objective=None, not_established=(), selected=(), deferred=(),
                plan_digest="", certificate_digest="",
                optimality=OPTIMALITY_UNAVAILABLE,
                optimality_detail=(
                    "the planner refused to propose a plan; there is nothing "
                    "to check and nothing to compare"),
                oracle_gap=None, mutation_survivals=(),
                mutations_applied=0,
            ),
            TimingRow(spec.name, candidate, replicate, planning_ns, 0),
        )
    planning_ns = clock() - start

    certificate = _certify(instance, plan, candidate, seed)
    start = clock()
    result: ScheduleCheckResult = check_schedule(instance, plan, certificate)
    checking_ns = clock() - start

    optimality = OPTIMALITY_UNAVAILABLE
    detail = ""
    gap: dict[str, int] | None = None
    survivals: tuple[str, ...] = ()
    mutations_applied = 0

    if result.accepted:
        survivals, mutations_applied = _mutation_survivals(
            instance, plan, candidate, seed)

    if not result.accepted or result.recomputed_objective is None:
        detail = (
            "optimality is only expressible over a candidate the checker "
            "found feasible with a recomputed value")
    elif oracle is None:
        detail = (
            "the oracle refused this instance "
            f"[{oracle_row.refusal_code}]; §9.1's fallback stands — this is "
            "a checked feasible candidate with a checked value and NO gap")
    else:
        gap = _gap(oracle.best_objective, result.recomputed_objective)
        if result.recomputed_objective != oracle.best_objective:
            optimality = OPTIMALITY_NOT_OPTIMUM
            detail = (
                "the checked value differs from the oracle optimum at some "
                "stage; the gap columns say where")
        else:
            bound = _certify(
                instance, plan, candidate, seed,
                artifact_digest=oracle.artifact.digest)
            verdict: OptimalityVerdict = check_optimality_artifact(
                instance,
                check_schedule(instance, plan, bound),
                oracle.artifact,
                certificate=bound,
            )
            if verdict.certified_optimal:
                optimality = OPTIMALITY_CERTIFIED
                detail = (
                    "the checker re-derived the optimum and the artifact "
                    "bound by this candidate's certificate matched at every "
                    "stage (§9.1)")
            else:
                optimality = OPTIMALITY_UNAVAILABLE
                detail = (
                    "the checked value equals the oracle optimum but the "
                    "artifact check REFUSED, so the word is not available: "
                    + ",".join(verdict.causes()))

    return (
        CandidateOutcome(
            instance=spec.name,
            candidate=candidate,
            replicate=replicate,
            status=(STATUS_CHECKED_FEASIBLE if result.accepted
                    else STATUS_REFUSED),
            causes=result.causes(),
            objective=result.recomputed_objective,
            not_established=result.not_established,
            selected=tuple(sorted(s.action_id for s in plan.selected)),
            deferred=tuple(sorted(plan.deferred)),
            plan_digest=plan.digest,
            certificate_digest=certificate.digest,
            optimality=optimality,
            optimality_detail=detail,
            oracle_gap=gap,
            mutation_survivals=survivals,
            mutations_applied=mutations_applied,
        ),
        TimingRow(spec.name, candidate, replicate, planning_ns, checking_ns),
    )


def _run_oracle(
    spec: InstanceSpec, instance: BatchInstance, manifest: E2Manifest
) -> tuple[OracleResult | None, OracleRow]:
    try:
        result = enumerate_optimum(
            instance,
            max_actions=manifest.oracle_max_actions,
            node_budget=manifest.oracle_node_budget,
        )
    except OracleRefused as refusal:
        code = str(refusal.data.get("code", "unnamed"))
        return None, OracleRow(
            instance=spec.name, admitted=False, refusal_code=code,
            best_objective=None, candidates_examined=None,
            feasible_candidates=None,
            note=(
                "the oracle refused rather than trimming its search; every "
                "candidate on this instance is reported as a CHECKED "
                "FEASIBLE candidate with no optimality claim and no gap"),
        )
    return result, OracleRow(
        instance=spec.name, admitted=True, refusal_code="",
        best_objective=result.best_objective,
        candidates_examined=result.candidates_examined,
        feasible_candidates=result.feasible_candidates,
        note="exhaustive enumeration inside the declared size guard",
    )


# --------------------------------------------------------------------------- #
# The report.                                                                  #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class E2Report:
    """The whole E2 comparison plus E3's synthetic mechanics.

    ``verdict`` is DERIVED, never authored. Timings are deliberately
    absent from :meth:`to_canonical_dict`: a preregistered comparison
    whose digest moved with machine load would not replay."""

    manifest_digest: str
    instances: tuple[Mapping[str, Any], ...]
    oracle_rows: tuple[OracleRow, ...]
    outcomes: tuple[CandidateOutcome, ...]
    comparator: ComparatorRow
    robustness: tuple[RobustnessRow, ...]
    falsifications: tuple[FalsificationRow, ...]
    deferred: Mapping[str, str]
    #: Realized wall-times. NOT part of the digested content.
    timings: tuple[TimingRow, ...]

    def fired(self) -> tuple[str, ...]:
        return tuple(f.clause for f in self.falsifications if f.fired)

    @property
    def verdict(self) -> str:
        if not self.outcomes:
            return "invalid:no_outcomes"
        if not any(o.status == STATUS_CHECKED_FEASIBLE for o in self.outcomes):
            return "invalid:no_checked_feasible_candidate"
        for row in self.falsifications:
            if row.fired and row.zero_tolerance:
                return f"refuted:{row.clause}"
        for row in self.falsifications:
            if row.fired:
                return f"performance_claim_rejected:{row.clause}"
        # Nothing fired. That is a clear run of WHAT RAN, and what ran is
        # not all of E2: the solver families, E2's history-window data
        # clause, and the comparator's own overhead term are deferred. A
        # bare "pass" would read as E2 passed, so the partiality is
        # carried IN the verdict, derived from the deferral rows.
        partial = tuple(
            name for name in PARTIAL_E2_DEFERRALS if name in self.deferred)
        if not partial:
            return VERDICT_PASS
        return f"{VERDICT_PASS_PARTIAL_E2}:{','.join(partial)}"

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "schema": "okg.synth.e2-batch-scheduler-report.v1",
            "manifest_digest": self.manifest_digest,
            "verdict": self.verdict,
            "checker_version": CHECKER_VERSION,
            "instances": [dict(sorted(i.items())) for i in self.instances],
            "oracle": [r.to_canonical_dict() for r in self.oracle_rows],
            "candidates": [o.to_canonical_dict() for o in self.outcomes],
            "comparator": self.comparator.to_canonical_dict(),
            "robustness": [r.to_canonical_dict() for r in self.robustness],
            "falsifications": [
                f.to_canonical_dict() for f in self.falsifications],
            "deferred": dict(sorted(self.deferred.items())),
            "timing_disposition": (
                "planning and checking wall-times are reported beside this "
                "object (E2Report.timings) and are deliberately NOT digested: "
                "a preregistered comparison whose digest moved with machine "
                "load would not be an exact replay (§12)"
            ),
        }

    def canonical_json(self) -> str:
        return json.dumps(
            self.to_canonical_dict(), sort_keys=True, separators=(",", ":")
        )

    def digest(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


#: The §8 stages, in order, as the deciding-stage histogram names them.
_STAGE_LABELS: tuple[str, ...] = (
    "stage_1_should_units",
    "stage_2_may_units",
    "stage_3_total_cost",
    "stage_4_makespan",
    "stage_5_tie_break",
)


def _deciding_stage(base: ObjectiveTuple, rival: ObjectiveTuple) -> str:
    """Which §8 stage actually settled this comparison.

    A stage with a zero count over the whole report is a stage the corpus
    does not exercise — reporting the histogram is what makes that
    visible instead of leaving it to be reverse-engineered."""
    deltas = (
        rival.should_units - base.should_units,
        rival.may_units - base.may_units,
        base.total_cost - rival.total_cost,
        base.makespan - rival.makespan,
    )
    for index, delta in enumerate(deltas):
        if delta != 0:
            return _STAGE_LABELS[index]
    if objective_sort_key(base) != objective_sort_key(rival):
        return _STAGE_LABELS[4]
    return "stage_0_identical"


def _checked_outcome_key(
    outcome: CandidateOutcome,
) -> tuple[Any, ...]:
    """What makes two candidates the SAME rule on one instance."""
    return (
        outcome.status,
        outcome.causes,
        outcome.selected,
        outcome.deferred,
        None if outcome.objective is None
        else objective_sort_key(outcome.objective),
    )


def _equivalent_candidates(
    by_instance: Mapping[str, Mapping[str, CandidateOutcome]],
    candidates: Sequence[str],
) -> tuple[tuple[str, ...], ...]:
    """Candidates whose checked outcome agrees on EVERY instance.

    Two such candidates are one selection rule implemented twice. They
    cannot corroborate each other, and a headline that counts both as
    winners counts one rule twice."""
    signature: dict[str, tuple[Any, ...]] = {}
    for candidate in candidates:
        signature[candidate] = tuple(
            (instance, _checked_outcome_key(by_instance[instance][candidate]))
            for instance in sorted(by_instance)
            if candidate in by_instance[instance]
        )
    groups: dict[tuple[Any, ...], list[str]] = {}
    for candidate in sorted(candidates):
        groups.setdefault(signature[candidate], []).append(candidate)
    return tuple(
        tuple(members) for _, members in sorted(groups.items(),
                                                key=lambda kv: kv[1])
        if len(members) > 1
    )


def _comparator_row(
    outcomes: Sequence[CandidateOutcome], threshold: Fraction
) -> ComparatorRow:
    """Compute the preregistered comparison. Nothing here is authored."""
    by_instance: dict[str, dict[str, CandidateOutcome]] = {}
    for outcome in outcomes:
        if outcome.candidate == RANDOM_CANDIDATE:
            continue
        by_instance.setdefault(outcome.instance, {})[outcome.candidate] = outcome

    rival_names = [name for name, _ in DETERMINISTIC_CANDIDATES
                   if name != COMPARATOR_CANDIDATE]
    tallies: dict[str, dict[str, int]] = {
        name: {"better": 0, "tied": 0, "worse": 0, "not_comparable": 0}
        for name in rival_names
    }

    compared = 0
    better = 0
    tied = 0
    worse = 0
    winners: dict[str, str] = {}
    stages: dict[str, int] = {}
    for instance in sorted(by_instance):
        rows = by_instance[instance]
        base = rows.get(COMPARATOR_CANDIDATE)
        if (base is None or base.status != STATUS_CHECKED_FEASIBLE
                or base.objective is None):
            winners[instance] = "not_compared:comparator_not_checkable"
            for name in rival_names:
                tallies[name]["not_comparable"] += 1
            continue
        base_key = objective_sort_key(base.objective)
        # Per candidate FIRST: every rival gets its own row on every
        # instance, whether or not it is the pool's best there.
        for name in rival_names:
            row = rows.get(name)
            if (row is None or row.status != STATUS_CHECKED_FEASIBLE
                    or row.objective is None):
                tallies[name]["not_comparable"] += 1
                continue
            key = objective_sort_key(row.objective)
            if key < base_key:
                tallies[name]["better"] += 1
            elif key == base_key:
                tallies[name]["tied"] += 1
            else:
                tallies[name]["worse"] += 1

        rivals = [
            row for name, row in sorted(rows.items())
            if name != COMPARATOR_CANDIDATE
            and row.status == STATUS_CHECKED_FEASIBLE
            and row.objective is not None
        ]
        if not rivals:
            winners[instance] = "not_compared:no_checkable_rival"
            continue
        compared += 1
        best_key = min(objective_sort_key(r.objective) for r in rivals)
        # Co-winners are RECORDED, not alphabetized away: naming only the
        # first minimal rival hides that several rules reached the value.
        best_names = "+".join(sorted(
            r.candidate for r in rivals
            if objective_sort_key(r.objective) == best_key))
        best = next(r for r in rivals
                    if objective_sort_key(r.objective) == best_key)
        stage = _deciding_stage(base.objective, best.objective)
        stages[stage] = stages.get(stage, 0) + 1
        if best_key < base_key:
            better += 1
            winners[instance] = best_names
        elif best_key == base_key:
            tied += 1
            winners[instance] = f"tie:{best_names}"
        else:
            worse += 1
            winners[instance] = COMPARATOR_CANDIDATE
    achieved = Fraction(better, compared) if compared else Fraction(0, 1)

    per_candidate = []
    for name in rival_names:
        tally = tallies[name]
        comparable = tally["better"] + tally["tied"] + tally["worse"]
        per_candidate.append(CandidateComparatorRow(
            candidate=name,
            better=tally["better"],
            tied=tally["tied"],
            worse=tally["worse"],
            not_comparable=tally["not_comparable"],
            # Written UNREDUCED: 0/14 and 2/14 say how much evidence
            # stands behind them; 0/1 and 1/7 do not.
            beats_comparator=f"{tally['better']}/{comparable}",
        ))

    equivalent = _equivalent_candidates(
        by_instance, [name for name, _ in DETERMINISTIC_CANDIDATES])
    return ComparatorRow(
        instances_compared=compared,
        instances_candidate_better=better,
        instances_tied=tied,
        instances_comparator_better=worse,
        threshold=f"{threshold.numerator}/{threshold.denominator}",
        achieved=f"{achieved.numerator}/{achieved.denominator}",
        met=bool(compared) and achieved >= threshold,
        per_instance_winner=winners,
        per_candidate=tuple(per_candidate),
        equivalent_candidates=equivalent,
        deciding_stages=stages,
        headline=_comparator_headline(
            better, compared, tuple(per_candidate), equivalent, stages),
    )


def _comparator_headline(
    better: int,
    compared: int,
    per_candidate: Sequence[CandidateComparatorRow],
    equivalent: Sequence[Sequence[str]],
    stages: Mapping[str, int],
) -> str:
    """The sentence the aggregate alone cannot say. DERIVED, not authored.

    It names how many DISTINCT rules cleared the baseline (candidates
    that agree on every instance count once), which candidates never
    cleared it, and which §8 stages never decided anything."""
    winners = [row.candidate for row in per_candidate if row.better > 0]
    classes: list[frozenset[str]] = []
    for candidate in winners:
        group = next(
            (frozenset(g) for g in equivalent if candidate in g),
            frozenset({candidate}))
        if group not in classes:
            classes.append(group)
    losers = [row.candidate for row in per_candidate if row.better == 0]
    unused = [label for label in _STAGE_LABELS if not stages.get(label)]
    duplicate = "; ".join(
        f"{' == '.join(group)} are one rule: identical checked outcome on "
        "every instance"
        for group in equivalent) or "no two candidates are equivalent"
    return (
        f"{len(classes)} distinct rule(s) strictly beat "
        f"{COMPARATOR_CANDIDATE} on {better} of {compared} comparable "
        f"instance(s), carried by {len(winners)} candidate(s) "
        f"[{', '.join(winners) or 'none'}]; "
        f"never ahead of it: [{', '.join(losers) or 'none'}]; "
        f"equivalence: {duplicate}; "
        f"§8 stages that decided nothing here: "
        f"[{', '.join(unused) or 'none'}]"
    )


def _clone_split_invariant(
    outcome: CandidateOutcome,
) -> tuple[str, tuple[int, ...] | None, tuple[str, ...]]:
    """What cloning or splitting an obligation must leave UNTOUCHED.

    The preregistered clause forbids "any objective improvement obtained
    merely by cloning (or splitting) an obligation". Coverage is counted
    in UNITS and both mutators are unit-preserving by construction, so
    comparing stages 1-2 alone compares a quantity that cannot move: the
    check would be closed under every mutation the witness can make. The
    stages where a clone-gained improvement can actually appear are COST
    (stage 3) and MAKESPAN (stage 4) — a rule that ranks by "obligations
    closed" rather than "units closed" pulls in a different, cheaper
    action once the relabelling arrives, and wins on cost while stages
    1-2 sit still.

    So the invariant is the whole checked objective through stage 4 PLUS
    the SELECTED SET: relabelling must move neither what is chosen nor
    what it is worth. The stage-5 tie-break is deliberately excluded — it
    is a canonical serialization of the plan, and the mutated instance
    has a different ``instance_id`` by construction. The status is
    carried too, so a variant that stops being checkable is a violation
    rather than a ``None == None`` pass."""
    objective = None if outcome.objective is None else (
        outcome.objective.should_units,
        outcome.objective.may_units,
        outcome.objective.total_cost,
        outcome.objective.makespan,
    )
    return (outcome.status, objective, outcome.selected)


def _invariant_detail(
    base: CandidateOutcome, variant: CandidateOutcome
) -> str:
    """Human-readable text for one clone/split row: every compared field."""
    base_value = _clone_split_invariant(base)
    variant_value = _clone_split_invariant(variant)
    return (
        f"status {base_value[0]} -> {variant_value[0]}; "
        f"(should,may,cost,makespan) {base_value[1]} -> {variant_value[1]}; "
        f"selected {list(base_value[2])} -> {list(variant_value[2])}"
    )


def _variant_outcome(
    spec: InstanceSpec,
    raw: Mapping[str, Any],
    candidate: str,
    replicate: str,
) -> CandidateOutcome:
    """Plan and check one candidate on a perturbed instance.

    A loader or planner refusal propagates to the caller, which reports
    it as a failed invariant — it is never swallowed into a pass."""
    instance = load_batch_instance(dict(raw))
    blank = OracleRow(spec.name, False, "not_run", None, None, None,
                      "robustness variants are compared candidate-to-"
                      "candidate; the oracle is not re-run")
    outcome, _ = _run_candidate(
        spec, instance, candidate, replicate, None, blank, lambda: 0)
    return outcome


def _run_robustness(
    specs: Sequence[InstanceSpec],
    outcomes: Sequence[CandidateOutcome],
    seed_probe: Mapping[tuple[str, str], tuple[set[str], bool]],
    manifest: E2Manifest,
) -> tuple[tuple[RobustnessRow, ...], dict[str, list[bool]]]:
    """E3's synthetic mechanics: clone, split, tier swap, seed stability."""
    baseline: dict[tuple[str, str], CandidateOutcome] = {
        (o.instance, o.candidate): o
        for o in outcomes
        if o.replicate == REPLICATE_NONE
    }
    rows: list[RobustnessRow] = []
    verdicts: dict[str, list[bool]] = {
        "clone_or_split_changes_coverage_value": [],
        "tier_boundary_swap_changes_must_set": [],
        "deterministic_candidate_unstable_across_seeds": [],
    }
    replicate = REPLICATE_NONE

    for spec in specs:
        obligations = list(spec.raw["obligations"])
        target = next(
            (o["id"] for o in obligations
             if o["priority"] in (PRIORITY_SHOULD, PRIORITY_MAY)), None)
        variants: list[tuple[str, Mapping[str, Any]]] = []
        if target is not None:
            variants.append(("clone", clone_obligation(spec.raw, target)))
            variants.append(("split", split_obligation(spec.raw, target)))
        swapped = swap_tier_boundary(spec.raw)

        for kind, raw in variants:
            for candidate, _ in DETERMINISTIC_CANDIDATES:
                base = baseline.get((spec.name, candidate))
                if base is None:
                    continue
                try:
                    variant = _variant_outcome(spec, raw, candidate, replicate)
                except (BatchLoadRefused, BaselineRefused) as refusal:
                    rows.append(RobustnessRow(
                        kind=kind, instance=spec.name, candidate=candidate,
                        detail=f"variant refused: {refusal}",
                        invariant_held=False))
                    verdicts[
                        "clone_or_split_changes_coverage_value"].append(False)
                    continue
                held = (_clone_split_invariant(variant)
                        == _clone_split_invariant(base))
                rows.append(RobustnessRow(
                    kind=kind, instance=spec.name, candidate=candidate,
                    detail=_invariant_detail(base, variant),
                    invariant_held=held))
                verdicts["clone_or_split_changes_coverage_value"].append(held)

        if swapped is not None:
            base_must = sorted(
                o["unit"]["subject_scope_key"] for o in obligations
                if o["priority"] == PRIORITY_MUST)
            swapped_must = sorted(
                o["unit"]["subject_scope_key"] for o in swapped["obligations"]
                if o["priority"] == PRIORITY_MUST)
            same_must = base_must == swapped_must
            for candidate, _ in DETERMINISTIC_CANDIDATES:
                base = baseline.get((spec.name, candidate))
                if base is None:
                    continue
                try:
                    variant = _variant_outcome(
                        spec, swapped, candidate, replicate)
                except (BatchLoadRefused, BaselineRefused) as refusal:
                    rows.append(RobustnessRow(
                        kind="tier_swap", instance=spec.name,
                        candidate=candidate,
                        detail=f"perturbed instance refused: {refusal}",
                        invariant_held=False))
                    verdicts[
                        "tier_boundary_swap_changes_must_set"].append(False)
                    continue
                base_mandatory_ok = (
                    CAUSE_MANDATORY_UNIT_UNCOVERED not in base.causes)
                variant_mandatory_ok = (
                    CAUSE_MANDATORY_UNIT_UNCOVERED not in variant.causes)
                held = same_must and base_mandatory_ok == variant_mandatory_ok
                rows.append(RobustnessRow(
                    kind="tier_swap", instance=spec.name, candidate=candidate,
                    detail=(
                        f"must units unchanged={same_must}; mandatory "
                        f"coverage {base_mandatory_ok} -> "
                        f"{variant_mandatory_ok}"),
                    invariant_held=held))
                verdicts["tier_boundary_swap_changes_must_set"].append(held)
        else:
            rows.append(RobustnessRow(
                kind="tier_swap", instance=spec.name, candidate="-",
                detail=(
                    "not applicable: the instance has no should/may pair to "
                    "swap. Reported, not counted as a pass"),
                invariant_held=True))

    # Selected-set stability. The deterministic arms are judged by the
    # seed-leak probe (same entry point, every replicate seed, byte-equal
    # plan digests); the random arm's spread is reported as the
    # measurement it is.
    for (instance, candidate), (digests, refused) in sorted(
            seed_probe.items()):
        stable = len(digests) == 1
        rows.append(RobustnessRow(
            kind="seed_stability", instance=instance, candidate=candidate,
            detail=(
                f"{len(digests)} distinct plan digest(s) across "
                f"{len(manifest.replicate_seeds)} replicate seeds"
                + (" (the arm refused under at least one seed, and refused "
                   "identically)" if refused else "")),
            invariant_held=stable))
        verdicts[
            "deterministic_candidate_unstable_across_seeds"].append(stable)

    random_sets: dict[str, set[tuple[str, ...]]] = {}
    for outcome in outcomes:
        if outcome.candidate != RANDOM_CANDIDATE:
            continue
        random_sets.setdefault(outcome.instance, set()).add(outcome.selected)
    for instance, seen in sorted(random_sets.items()):
        rows.append(RobustnessRow(
            kind="seed_stability", instance=instance,
            candidate=RANDOM_CANDIDATE,
            detail=(
                f"diagnostic arm: {len(seen)} distinct selected set(s) "
                f"across {len(manifest.replicate_seeds)} seeds — the spread "
                "IS the measurement here, not a defect"),
            invariant_held=True))
    return tuple(rows), verdicts


def run_report(
    manifest: E2Manifest, *, clock: Clock = time.perf_counter_ns
) -> E2Report:
    """Run the whole preregistered E2 grid plus E3's synthetic mechanics.

    For each instance: the oracle once (or its named refusal), then every
    candidate at every replicate seed, each plan validated by the
    independent checker. Deterministic arms are replanned at every seed
    on purpose — that is how the stability clause is evaluated."""
    assert_no_cardinal_utility_inputs()
    specs = instance_specs(manifest)
    threshold_text = manifest.comparator_threshold.split("/")
    threshold = Fraction(int(threshold_text[0]), int(threshold_text[1]))

    instances: list[Mapping[str, Any]] = []
    oracle_rows: list[OracleRow] = []
    outcomes: list[CandidateOutcome] = []
    timings: list[TimingRow] = []
    replay_digests: dict[tuple[str, str, str], str] = {}
    tie_break_unstable: list[str] = []
    seed_probe: dict[tuple[str, str], tuple[set[str], bool]] = {}

    for spec in specs:
        instance = spec.load()
        oracle, oracle_row = _run_oracle(spec, instance, manifest)
        oracle_rows.append(oracle_row)
        instances.append({
            "name": spec.name,
            "instance_id": instance.instance_id,
            "source": spec.source,
            "actions": len(instance.actions),
            "obligations": len(instance.obligations),
            "resources": len(instance.resources),
            "instance_digest": instance.digest,
            "unit_mapping_digest": instance.unit_mapping_digest,
            "exercises": spec.exercises,
            "straddles_oracle_guard": spec.straddles_oracle_guard,
        })
        # A deterministic arm takes no seed, so running it once per
        # replicate would repeat an identical plan — and, worse, repeat
        # the checker's expensive optimum re-derivation — without adding
        # one bit of information. It is run once here, and the claim that
        # it really is seed-independent is evaluated separately and
        # explicitly by the seed-leak probe below.
        arms = [(name, REPLICATE_NONE) for name, _ in DETERMINISTIC_CANDIDATES]
        arms += [(RANDOM_CANDIDATE, str(seed))
                 for seed in manifest.replicate_seeds]
        for candidate, replicate in arms:
            outcome, timing = _run_candidate(
                spec, instance, candidate, replicate, oracle, oracle_row,
                clock)
            outcomes.append(outcome)
            timings.append(timing)
            # Determinism: replan the same (instance, candidate, seed)
            # and require a byte-equal plan digest.
            repeat, _ = _run_candidate(
                spec, instance, candidate, replicate, None,
                OracleRow(spec.name, False, "not_run", None, None, None,
                          "replay probe"),
                lambda: 0)
            replay_digests[(spec.name, candidate, replicate)] = (
                repeat.plan_digest)
            if repeat.plan_digest != outcome.plan_digest:
                tie_break_unstable.append(
                    f"{spec.name}/{candidate}/{replicate}")

        # The seed-leak probe: plan every deterministic arm through the
        # SAME seed-taking entry point under every replicate seed. A
        # deterministic arm that quietly grew a seed dependence shows up
        # here as two distinct plan digests.
        for candidate, _ in DETERMINISTIC_CANDIDATES:
            digests: set[str] = set()
            refused = False
            for seed in manifest.replicate_seeds:
                try:
                    digests.add(
                        _plan_candidate(instance, candidate, str(seed)).digest)
                except BaselineRefused as refusal:
                    refused = True
                    digests.add(f"refused:{refusal.data.get('code')}")
            seed_probe[(spec.name, candidate)] = (digests, refused)

    robustness, verdicts = _run_robustness(
        specs, outcomes, seed_probe, manifest)
    comparator = _comparator_row(outcomes, threshold)

    accepted = [o for o in outcomes if o.status == STATUS_CHECKED_FEASIBLE]
    bad_publishable = [
        f"{o.instance}/{o.candidate}" for o in accepted
        if CAUSE_MANDATORY_UNIT_UNCOVERED in o.causes
    ]
    survivals = [
        f"{o.instance}/{o.candidate}:{name}"
        for o in outcomes for name in o.mutation_survivals
    ]

    falsifications = (
        FalsificationRow(
            clause="publishable_plan_with_unresolved_must",
            fired=bool(bad_publishable),
            zero_tolerance=True,
            evaluated=len(accepted),
            detail=(
                "no accepted candidate carries a mandatory_unit_uncovered "
                "finding" if not bad_publishable
                else "ACCEPTED WITH UNRESOLVED MUST: "
                     + ",".join(sorted(bad_publishable))),
        ),
        FalsificationRow(
            clause="checker_accepts_mutated_plan",
            fired=bool(survivals),
            zero_tolerance=True,
            evaluated=sum(o.mutations_applied for o in accepted),
            detail=(
                "every applicable mutation of every accepted plan was "
                "refused" if not survivals
                else "MUTANT ACCEPTED: " + ",".join(sorted(survivals))),
        ),
        FalsificationRow(
            clause="nondeterministic_tie_break",
            fired=bool(tie_break_unstable),
            zero_tolerance=True,
            evaluated=len(replay_digests),
            detail=(
                "every candidate replanned to a byte-equal plan digest"
                if not tie_break_unstable
                else "REPLAN DIVERGED: " + ",".join(sorted(tie_break_unstable))
            ),
        ),
        FalsificationRow(
            clause="clone_or_split_changes_coverage_value",
            fired=not all(verdicts["clone_or_split_changes_coverage_value"]),
            zero_tolerance=True,
            evaluated=len(verdicts["clone_or_split_changes_coverage_value"]),
            detail=(
                "cloning and splitting an obligation left every candidate's "
                "checked coverage value unchanged"
                if all(verdicts["clone_or_split_changes_coverage_value"])
                else "CLONE OR SPLIT MOVED A COVERAGE VALUE; see the "
                     "robustness rows"),
        ),
        FalsificationRow(
            clause="tier_boundary_swap_changes_must_set",
            fired=not all(verdicts["tier_boundary_swap_changes_must_set"]),
            zero_tolerance=True,
            evaluated=len(verdicts["tier_boundary_swap_changes_must_set"]),
            detail=(
                "swapping the should/may boundary changed neither the must "
                "unit set nor any candidate's mandatory-coverage outcome"
                if all(verdicts["tier_boundary_swap_changes_must_set"])
                else "MUST DISAGREEMENT under a should/may perturbation"),
        ),
        FalsificationRow(
            clause="deterministic_candidate_unstable_across_seeds",
            fired=not all(
                verdicts["deterministic_candidate_unstable_across_seeds"]),
            zero_tolerance=True,
            evaluated=len(
                verdicts["deterministic_candidate_unstable_across_seeds"]),
            detail=(
                "every deterministic candidate produced one selected set "
                "across all replicate seeds"
                if all(verdicts[
                    "deterministic_candidate_unstable_across_seeds"])
                else "A DETERMINISTIC ARM MOVED WITH THE SEED"),
        ),
        FalsificationRow(
            clause="simple_baseline_comparator_not_beaten",
            fired=not comparator.met,
            zero_tolerance=False,
            evaluated=comparator.instances_compared,
            detail=(
                f"non-FIFO candidates strictly beat stable FIFO on "
                f"{comparator.achieved} of comparable instances against a "
                f"preregistered threshold of {comparator.threshold}. "
                f"{comparator.headline}"),
        ),
    )

    return E2Report(
        manifest_digest=manifest.digest(),
        instances=tuple(instances),
        oracle_rows=tuple(oracle_rows),
        outcomes=tuple(outcomes),
        comparator=comparator,
        robustness=robustness,
        falsifications=falsifications,
        deferred=dict(manifest.deferred),
        timings=tuple(timings),
    )
