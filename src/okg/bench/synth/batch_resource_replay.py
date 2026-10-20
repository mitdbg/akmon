"""E4 named-capacity and lane replay: shared pools versus critical/light.

PACT ``concord-disclosure-budget``, task.batch.resource-replay.
Normative sections: ``designs/batch-scheduling-research.md`` §7.2
(resources and lanes), §11 preamble (preregistration discipline), §11 E4
(the configuration grid, the metrics, and the falsification clause), and
§12 (reproducibility). The E2 lane
(:mod:`okg.bench.synth.batch_experiments`) is the reference idiom for the
corpus builders, the preregistered manifest with a pinned digest, the
derived verdict, and the "a deferral is a row, never a silent cap" rule;
this module follows it and adds nothing to it.

What this module is
-------------------
An experiment harness for ONE question: does a critical/light lane
SPLIT of a named pool do anything a generic shared pool of the same
total capacity does not? It builds a base workload corpus, compiles each
workload under every §11 E4 resource configuration, asks each candidate
planner for a plan, and hands every plan to the INDEPENDENT checker.

:func:`~...batch_schedule_checker.check_schedule` is the only validity
and capacity authority here. This module never decides whether a
capacity holds, never re-derives an objective, and never softens a
refusal. Where a metric needs a feasibility judgement — the starvation
probe below is the whole of that category — the judgement is obtained by
handing the checker a plan and reading its verdict, not by re-deriving
capacity legality locally. Utilisation sums DECLARED capacity from the
trusted-base schema accessor
:meth:`~...batch_schedule.Resource.capacity_at`; that is arithmetic over
a declaration, not a legality check.

The word "optimal" does not appear in this module's vocabulary and no
oracle arm runs: §11 E4's metric list names none, and §9.1 permits the
word only through a checked artifact.

Lanes without schema support: the compilation
---------------------------------------------
Design §7.2 is explicit that ``critical`` and ``light`` are "optional
configuration aliases or eligibility rules over those pools" and "are
NOT schema-level action kinds", and the frozen schema carries the design
through: :class:`~...batch_schedule.Resource` is a named pool and
nothing else. There is therefore no lane field to set, and this module
does not invent one. A lane configuration is CONFIGURATION, and it is
made real by COMPILING it into a plain named-pool instance the frozen
schema already expresses:

* **Eligibility.** A lane rule partitions the workload's actions into a
  critical-eligible set and a light set. The reviewed rule
  (:data:`LANE_RULE_MANDATORY_CLOSURE`) makes the critical-eligible set
  exactly the full prerequisite closure of every action covering a
  ``must`` obligation, which is what §7.2 requires a configured critical
  lane to admit. The rule is not trusted to be right: every compiled
  lane configuration is checked against that requirement before it runs,
  and a configuration that leaves any member of the mandatory closure
  outside the critical lane is graded INVALID
  (:data:`CLAUSE_LANE_EXCLUDES_MANDATORY_CLOSURE`), never planned around.
* **Borrowing OFF** compiles to two pools, ``critical`` at the critical
  share and ``light`` at the light share. Each action's demand is
  rewritten to its eligible pool and written as ``0`` on the other —
  written, not omitted, because §7 makes an absent demand key a refusal
  and the corpus does not lean on that refusal.
* **Borrowing ON** compiles to ONE pool, ``lane_pool``, at the SUM of
  the two shares, plus a :class:`~...batch_schedule.ConflictGroup` over
  the light-lane actions whose ``max_concurrent`` is the light share.
  That is exactly the asymmetric policy the design's mandatory-lane
  protection implies: a critical action may draw on capacity the light
  lane is not using (up to the whole pool), while the light lane can
  never exceed its own share. The checker validates it with no schema
  change at all — group membership is evaluated at every schedule event,
  and pool capacity at every event AND every capacity breakpoint.

Two things about that compilation are NOT free, and both are rows rather
than silences:

* the borrowing-ON encoding states the light lane's share as a COUNT of
  concurrent members, so it is exact only while lane-arm demands are one
  unit per action (:data:`DEFERRED` ``unit_demand_lane_compilation``);
  the heterogeneous control, which has no lanes, carries genuine
  per-pool demands.
* **symmetric** borrowing — the light lane drawing on idle critical
  capacity as well — is NOT expressible. The frozen schema fixes
  ``demand(a, r)`` as a constant per ``(action, resource)`` pair and has
  no disjunctive or state-dependent demand, so "draw from whichever pool
  has room" has nowhere to live; a combined pool with no light cap does
  not express it either, because that is just the shared pool. It is
  DEFERRED by name (:data:`DEFERRED` ``symmetric_capacity_borrowing``)
  with the gap stated, and it is one of the deferrals the derived
  verdict carries.

Preemption gets the same treatment. §7 says actions are non-preemptive
unless the instance decomposes them into explicit chunks with their own
identities, dependencies, effects, and demands; §11 E4 declares no
preemption policy; nothing here preempts, and the route the schema does
offer is named in :data:`DEFERRED` rather than approximated.

The three timing metrics, defined before they were measured
-----------------------------------------------------------
§11 E4 names "queue delay", "mandatory prerequisite delay", and
"starvation" without defining them, so they are defined here, in the
preregistered manifest, and derived deterministically from plan timing:

* **queue delay** of a selected action is ``start - ready``, where
  ``ready`` is the latest end among its prerequisites in the same plan,
  or ``0`` for an action with none. It is measured from dependency
  readiness because the frozen instance has no release or arrival time
  to measure from — a limit named in :data:`DEFERRED`.
* **mandatory prerequisite delay** is the same quantity restricted to
  the mandatory closure: the actions §7.2 says a critical lane must
  admit.
* **starvation** is NOT a threshold on delay. A delay can be honest
  work-conservation, and a threshold on it would be a number nobody
  reviewed. It is measured by TWO probes, because there are two ways a
  mandatory prerequisite can be made to wait and one probe can only see
  one of them. Both hand the counterfactual to the checker, so "could
  have run" means exactly what capacity, hard caps, conflicts, groups,
  and prerequisites already mean, and this module adds no second opinion
  about them. Both thresholds are ZERO, because §7.2 does not offer a
  budget: "a configuration that starves a mandatory prerequisite is
  invalid".

  * **single-move starvation** — the action waited on IDLE capacity:
    some integer ``t`` in ``[ready, start)`` yields a plan, identical
    but for moving that ONE action to ``t``, that the checker accepts.
    The name carries the bound: it displaces nothing, so under a
    work-conserving planner it cannot see the second case at all.
  * **inversion starvation** — the action waited BEHIND lower-priority
    work: a strictly-lower-priority action (one outside the mandatory
    closure) was running in the interval it waited through, and the
    DISPLACED plan — the mandatory action at that time, the
    lower-priority work delayed by its duration — is accepted by the
    checker. Its zero-tolerance clause is SCOPED to lane configurations,
    from §7.2's own conditional: "If a critical lane is configured, it
    must admit the full prerequisite closure of mandatory actions". A
    generic shared pool configures no lane and promises nothing about
    one, so an inversion there is a reported measurement, not an invalid
    configuration. Under a lane configuration the promise is live: a
    critical lane that admits the closure on paper and then lets light
    work hold the capacity has starved it in fact.

Determinism
-----------
Every draw goes through :class:`~okg.bench.synth.prf.SynthPrf`. There is
no ``random``. This module reads NO clock at all — §11 E4's metric list
contains no timing, so there is nothing to report beside the digest and
nothing that could leak into it — and two runs produce a byte-equal
report.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from fractions import Fraction
from typing import Any, Callable, Mapping, Sequence

from okg.bench.synth.batch_experiments import (
    plan_coverage_per_cost_greedy,
    plan_tier_earliest_finish,
)
from okg.bench.synth.prf import SynthPrf
from okg.substrate.consistency.batch_schedule import (
    ARTIFACT_UNBOUND,
    CONTRACT_SCHEMA_VERSION,
    PRIORITY_MAY,
    PRIORITY_MUST,
    PRIORITY_SHOULD,
    TERMINATION_EXHAUSTED,
    BatchInstance,
    ObjectiveTuple,
    ProducerRef,
    SchedulePlan,
    Unknown,
    canonical_schedule_serialization,
    load_batch_instance,
    object_digest,
    objective_sort_key,
)
from okg.substrate.consistency.batch_schedule_baselines import (
    BaselineRefused,
    plan_dependency_tier_greedy,
    plan_fifo,
    plan_marginal_coverage_greedy,
)
from okg.substrate.consistency.batch_schedule_checker import (
    CAUSE_CAPACITY_EXCEEDED,
    CAUSE_HARD_CAP_EXCEEDED,
    CAUSE_RESERVATION_EXCEEDED,
    CHECKER_VERSION,
    ScheduleCheckResult,
    check_schedule,
)
from okg.substrate.consistency.batch_schedule_oracle import build_certificate
from okg.substrate.errors import SubstrateError

__all__ = [
    "BORROWING_NOT_APPLICABLE",
    "BORROWING_OFF",
    "BORROWING_ON_ASYMMETRIC",
    "CANDIDATES",
    "CAPACITY_VIOLATION_CAUSES",
    "CLAUSES",
    "CLAUSE_CAPACITY_MATCHED_COMPARISON",
    "CLAUSE_CHECKER_REFUSAL_READ_AS_PASS",
    "CLAUSE_LANE_EXCLUDES_MANDATORY_CLOSURE",
    "CLAUSE_MANDATORY_PREREQUISITE_INVERSION",
    "CLAUSE_MANDATORY_PREREQUISITE_STARVED",
    "CLAUSE_REALIZED_CAPACITY_VIOLATION",
    "COMPARISON_BINDING_FIELDS",
    "COMPARISON_STAGES",
    "CONCLUSION_HARDCODING_SUPPORTED",
    "CONCLUSION_NOT_ESTABLISHED",
    "CONCLUSION_NOT_ESTABLISHED_INVALID",
    "CONCLUSION_NOT_HARDCODED",
    "COST_UNIT",
    "DEFAULT_LANE_THRESHOLD",
    "DEFAULT_SYNTHETIC_SEEDS",
    "DEFERRED",
    "DESIGN_NAMED_CONFIGURATIONS",
    "E4Manifest",
    "E4Report",
    "HAND_BUILT_WORKLOADS",
    "KIND_HETEROGENEOUS",
    "KIND_LANE",
    "KIND_SHARED",
    "LANE_CONFIG_EXCLUDES_MANDATORY_CLOSURE",
    "LANE_CONFIG_VALID",
    "LANE_CRITICAL",
    "LANE_LIGHT",
    "LANE_NOT_APPLICABLE",
    "LANE_RULE_ACTION_CLASS",
    "LANE_RULE_MANDATORY_CLOSURE",
    "LANE_RULE_NONE",
    "LANE_SHARES",
    "MINIMUM_COMPARABLE_PAIRS",
    "PARTIAL_E4_DEFERRALS",
    "PREREGISTERED_MANIFEST_DIGEST",
    "PRODUCER_FAMILY",
    "PRODUCER_VERSION",
    "REPLAY_ROSTER_VERSION",
    "STARVATION_THRESHOLD",
    "STATUS_CHECKED_FEASIBLE",
    "STATUS_PLANNER_REFUSED",
    "STATUS_REFUSED",
    "UNIT_MAPPING_VERSION",
    "VERDICT_PASS",
    "VERDICT_PASS_PARTIAL_E4",
    "VERDICT_REFUSALS_SUFFIX",
    "ZERO_TOLERANCE",
    "BatchResourceReplayError",
    "CapacityEventWitness",
    "ComparisonRow",
    "ConfigComparisonRow",
    "FalsificationRow",
    "ReplayOutcome",
    "ResourceConfig",
    "Workload",
    "build_manifest",
    "capacity_event_witness",
    "capacity_matched_pairs",
    "compile_instance",
    "configurations",
    "hand_built_workloads",
    "inversion_starved_actions",
    "lane_assignment",
    "lane_config_validity",
    "mandatory_closure",
    "run_report",
    "single_move_starved_actions",
    "synthetic_workload",
    "workloads",
]


class BatchResourceReplayError(SubstrateError):
    """The harness refused. A mis-built experiment proves nothing."""


# --------------------------------------------------------------------------- #
# The ONE local canonicalization (EWD-BRR-5).                                  #
# --------------------------------------------------------------------------- #
#: The two markers the local dialect adds to the trusted base's canonical
#: closure, and NOTHING else. They are objects rather than strings so they
#: cannot collide with any string a report row could carry: the base's
#: closure admits ``str`` and ``int``, so a bare ``"true"`` sentinel would
#: be indistinguishable from a field whose value really is the text
#: "true". A mapping that literally carried one of these keys WOULD
#: collide, so the canonicalizer refuses those keys by name below.
_LOCAL_BOOL_KEY = "$e4_bool"
_LOCAL_ABSENT_KEY = "$e4_absent"
_LOCAL_RESERVED_KEYS = (_LOCAL_BOOL_KEY, _LOCAL_ABSENT_KEY)


def _local_canonical(value: Any, where: str = "$") -> Any:
    """Everything the trusted base's canonical closure takes, plus two.

    :func:`~...batch_schedule.canonical_json` admits integers and strings
    (inside mappings and sequences) and REFUSES ``None``, ``bool``,
    ``float``, and every other type. This module digests REPORT objects
    rather than contract objects, and a report row carries exactly two
    values that closure cannot take:

    * a **boolean** — ``design_named`` is a yes/no about the design text,
      and spelling it ``"yes"``/``"no"`` purely to reach the base's
      function would make the row worse to read for no gain in binding
      strength. It is encoded as ``{"$e4_bool": "true"|"false"}``.
    * an **honest absence** — ``shares`` is ``None`` for a configuration
      that declares no lanes, which is a stated "not applicable", not an
      unknown quantity and not a zero. It is encoded as
      ``{"$e4_absent": "not_stated"}``.

    Every refusal the base makes for a reason this module shares is
    re-imposed verbatim: floats (including NaN and the infinities) are
    refused because a float in digested content is a digest that moves
    between machines, and any other type is refused rather than
    stringified — a ``default=str`` fallback collides
    ``Fraction(1, 2)`` with the string ``"1/2"``, and this function has
    no fallback at all."""
    if value is None:
        return dict([(_LOCAL_ABSENT_KEY, "not_stated")])
    if isinstance(value, bool):
        return dict([(_LOCAL_BOOL_KEY, "true" if value else "false")])
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return value
    if isinstance(value, float):
        raise BatchResourceReplayError(
            "the local digest dialect refuses floats exactly as the trusted "
            "base does; a float in digested content is a digest that moves "
            "between machines, and NaN is not even JSON",
            data={"where": where, "type": type(value).__name__},
        )
    if isinstance(value, Mapping):
        out: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise BatchResourceReplayError(
                    "mapping keys must be strings in the local digest dialect",
                    data={"where": where, "key_type": type(key).__name__},
                )
            if key in _LOCAL_RESERVED_KEYS:
                raise BatchResourceReplayError(
                    "a report mapping may not carry a reserved local-dialect "
                    "marker key; that would collide with the boolean or "
                    "absence encoding",
                    data={"where": where, "key": key},
                )
            out[key] = _local_canonical(item, f"{where}.{key}")
        return out
    if isinstance(value, (list, tuple)):
        return [_local_canonical(item, f"{where}[{index}]")
                for index, item in enumerate(value)]
    raise BatchResourceReplayError(
        "the local digest dialect has NO stringifying fallback; an "
        "unadmitted type is refused, never rendered with str()",
        data={"where": where, "type": type(value).__name__},
    )


def _local_canonical_json(value: Any) -> str:
    """The single local canonical rendering. Both local digests use it."""
    return json.dumps(
        _local_canonical(value), sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False,
    )


def _local_digest(value: Any) -> str:
    """sha256 over :func:`_local_canonical_json`."""
    return hashlib.sha256(
        _local_canonical_json(value).encode("utf-8")).hexdigest()


#: §12 producer identity. Bound to THIS experiment: a plan produced for
#: E4 must not be traceable to E2's producer string, and a certificate
#: that names this version is this lane's certificate.
PRODUCER_VERSION = "okg.bench.synth.batch-resource-replay/1"
PRODUCER_FAMILY = "untrusted-baseline-family"

#: The candidate roster's own version. It is a comparison-binding field
#: (see :data:`COMPARISON_BINDING_FIELDS`): two arms planned by different
#: rosters are not a capacity-matched comparison, they are two
#: experiments.
REPLAY_ROSTER_VERSION = "e4-roster/1"

COST_UNIT = "tokens"
UNIT_MAPPING_VERSION = "e4-unit-map/1"


# --------------------------------------------------------------------------- #
# The candidate roster. Same untrusted proposal generators as E2, bound        #
# here to E4's own producer and roster identity.                               #
# --------------------------------------------------------------------------- #
CANDIDATES: tuple[tuple[str, Callable[[BatchInstance], SchedulePlan]], ...] = (
    ("fifo", plan_fifo),
    ("dependency_tier_greedy", plan_dependency_tier_greedy),
    ("marginal_coverage_greedy", plan_marginal_coverage_greedy),
    ("coverage_per_cost_greedy", plan_coverage_per_cost_greedy),
    ("tier_earliest_finish", plan_tier_earliest_finish),
)

STATUS_CHECKED_FEASIBLE = "checked_feasible"
STATUS_REFUSED = "refused"
STATUS_PLANNER_REFUSED = "planner_refused"

#: The checker causes that ARE a capacity violation. Imported, never
#: restated: a local copy of a cause string would keep matching after the
#: checker renamed it.
CAPACITY_VIOLATION_CAUSES: tuple[str, ...] = (
    CAUSE_CAPACITY_EXCEEDED,
    CAUSE_HARD_CAP_EXCEEDED,
    CAUSE_RESERVATION_EXCEEDED,
)


# --------------------------------------------------------------------------- #
# Lanes are CONFIGURATION (§7.2), so they are spelled out here and             #
# compiled away before any instance exists.                                    #
# --------------------------------------------------------------------------- #
LANE_CRITICAL = "critical"
LANE_LIGHT = "light"
LANE_NOT_APPLICABLE = "-"

#: The reviewed eligibility rule: critical-eligible is exactly the full
#: prerequisite closure of the mandatory actions (§7.2).
LANE_RULE_MANDATORY_CLOSURE = "mandatory_closure"
#: A DIAGNOSTIC eligibility rule that assigns by ``action_class``. It is
#: not part of the reviewed grid. It exists so the invalidity falsifier
#: has something real to fire on: a class-based rule splits a mandatory
#: closure that spans classes, which is precisely the configuration §7.2
#: calls invalid.
LANE_RULE_ACTION_CLASS = "action_class"
LANE_RULE_NONE = "none"

BORROWING_OFF = "off"
BORROWING_ON_ASYMMETRIC = "on_asymmetric"
BORROWING_NOT_APPLICABLE = "not_applicable"

KIND_SHARED = "shared_pool"
KIND_LANE = "lane_split"
KIND_HETEROGENEOUS = "heterogeneous"

#: The action classes the heterogeneous control maps onto its pools. A
#: declared map, not a draw: the control exists to show that the whole
#: apparatus works over more than one named pool with UNEQUAL demands.
HETEROGENEOUS_DEMAND: Mapping[str, Mapping[str, int]] = {
    "check": {"cpu": 1, "db": 0, "model": 0},
    "repair": {"cpu": 1, "db": 1, "model": 0},
    "probe": {"cpu": 1, "db": 0, "model": 1},
    "gate": {"cpu": 1, "db": 1, "model": 1},
}


@dataclass(frozen=True)
class ResourceConfig:
    """One §11 E4 resource configuration, before any workload meets it.

    A configuration is pure configuration: pool ids with their
    piecewise-constant capacity segments, an eligibility rule, and a
    borrowing policy. :func:`compile_instance` is the only thing that
    turns it into a :class:`~...batch_schedule.BatchInstance`."""

    name: str
    kind: str
    #: pool id -> ascending capacity segments, as the schema spells them.
    pools: Mapping[str, tuple[Mapping[str, int], ...]]
    lane_rule: str
    borrowing: str
    #: The (critical, light) shares for a lane configuration; ``None``
    #: for the shared and heterogeneous arms.
    shares: tuple[int, int] | None
    #: Total instantaneous capacity at time 0, the quantity the
    #: capacity-MATCHED comparison matches on.
    total_capacity: int
    #: ``True`` for a configuration §11 E4 names verbatim.
    design_named: bool
    exercises: str

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": self.kind,
            "pools": {
                pool: [dict(sorted(s.items())) for s in segments]
                for pool, segments in sorted(self.pools.items())
            },
            "lane_rule": self.lane_rule,
            "borrowing": self.borrowing,
            "shares": (None if self.shares is None else list(self.shares)),
            "total_capacity": self.total_capacity,
            "design_named": self.design_named,
            "exercises": self.exercises,
        }

    @property
    def digest(self) -> str:
        # ONE local dialect, defined and justified once at
        # :func:`_local_canonical`: a configuration is a REPORT object,
        # not a contract object, and it carries the two things the
        # contract's canonical closure refuses — a boolean and an honest
        # absence. Every OTHER refusal the base makes is re-imposed.
        return _local_digest(self.to_canonical_dict())


def _flat(capacity: int) -> tuple[Mapping[str, int], ...]:
    return ({"start": 0, "capacity": capacity},)


def _shared(capacity: int, *, design_named: bool) -> ResourceConfig:
    return ResourceConfig(
        name=f"shared_pool_{capacity}",
        kind=KIND_SHARED,
        pools={"pool": _flat(capacity)},
        lane_rule=LANE_RULE_NONE,
        borrowing=BORROWING_NOT_APPLICABLE,
        shares=None,
        total_capacity=capacity,
        design_named=design_named,
        exercises=(
            "§11 E4's generic shared pool at capacity "
            f"{capacity}" if design_named else
            "a shared pool at the total capacity of a lane split, present "
            "so the preregistered capacity-MATCHED comparison has a "
            "partner at equal total capacity"
        ),
    )


def _lane(critical: int, light: int, borrowing: str) -> ResourceConfig:
    total = critical + light
    if borrowing == BORROWING_OFF:
        pools = {LANE_CRITICAL: _flat(critical), LANE_LIGHT: _flat(light)}
    else:
        pools = {"lane_pool": _flat(total)}
    return ResourceConfig(
        name=f"lane_{critical}_{light}_borrow_{borrowing}",
        kind=KIND_LANE,
        pools=pools,
        lane_rule=LANE_RULE_MANDATORY_CLOSURE,
        borrowing=borrowing,
        shares=(critical, light),
        total_capacity=total,
        design_named=True,
        exercises=(
            f"§11 E4's separate aliases (critical, light) = ({critical}, "
            f"{light}) with borrowing {borrowing}. Borrowing OFF compiles "
            "to two named pools; borrowing ON compiles to one pool at the "
            "summed capacity plus a light-lane concurrency cap, so a "
            "critical action may draw on idle light capacity and the light "
            "lane may never exceed its share"
        ),
    )


def _heterogeneous(db: int) -> ResourceConfig:
    return ResourceConfig(
        name=f"heterogeneous_cpu4_db{db}_model1",
        kind=KIND_HETEROGENEOUS,
        pools={
            "cpu": _flat(4), "db": _flat(db), "model": _flat(1),
        },
        lane_rule=LANE_RULE_NONE,
        borrowing=BORROWING_NOT_APPLICABLE,
        shares=None,
        total_capacity=4 + db + 1,
        design_named=True,
        exercises=(
            "§11 E4's heterogeneous control {cpu:4, db:" f"{db}" ", model:1}. "
            "It carries genuine UNEQUAL per-pool demands, so the named-pool "
            "machinery is exercised outside the unit-demand lane encoding"
        ),
    )


#: The §11 E4 configuration names, verbatim from the design's list. A
#: test enumerates the grid against this tuple, so a configuration cannot
#: be dropped without the drop being visible.
DESIGN_NAMED_CONFIGURATIONS: tuple[str, ...] = (
    "shared_pool_1",
    "shared_pool_2",
    "shared_pool_4",
    "lane_1_1_borrow_off",
    "lane_1_2_borrow_off",
    "lane_2_2_borrow_off",
    "lane_2_4_borrow_off",
    "lane_1_1_borrow_on_asymmetric",
    "lane_1_2_borrow_on_asymmetric",
    "lane_2_2_borrow_on_asymmetric",
    "lane_2_4_borrow_on_asymmetric",
    "heterogeneous_cpu4_db1_model1",
    "heterogeneous_cpu4_db2_model1",
)

#: The lane shares §11 E4 names, in its order.
LANE_SHARES: tuple[tuple[int, int], ...] = ((1, 1), (1, 2), (2, 2), (2, 4))


def configurations() -> tuple[ResourceConfig, ...]:
    """The whole E4 grid, in canonical order.

    Every configuration §11 E4 names, plus the shared pools at capacity 3
    and 6. Those two are not decoration: E4's falsifier is a
    CAPACITY-MATCHED comparison with the generic shared pool, and the
    splits (1,2) and (2,4) have no partner at equal total capacity among
    the design's own 1/2/4. They are marked ``design_named=False`` so a
    reader can tell an addition from a quotation."""
    named_shared = {1, 2, 4}
    matched = {c + light for c, light in LANE_SHARES}
    grid: list[ResourceConfig] = [
        _shared(capacity, design_named=capacity in named_shared)
        for capacity in sorted(named_shared | matched)
    ]
    for borrowing in (BORROWING_OFF, BORROWING_ON_ASYMMETRIC):
        for critical, light in LANE_SHARES:
            grid.append(_lane(critical, light, borrowing))
    grid.append(_heterogeneous(1))
    grid.append(_heterogeneous(2))
    return tuple(grid)


# --------------------------------------------------------------------------- #
# The base workload corpus. Resource-free by construction: a workload          #
# says what work exists, a configuration says what it may run on.              #
# --------------------------------------------------------------------------- #
def _unit(name: str) -> dict[str, Any]:
    return {
        "spec_version": "e4-spec/1",
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


def _work(
    action_id: str,
    *,
    action_class: str = "check",
    covers: Sequence[str] = (),
    requires: Sequence[str] = (),
    conflicts: Sequence[str] = (),
    duration: int = 1,
    execution_cost: int = 1,
) -> dict[str, Any]:
    """One unit of work, with NO demand: demand is the configuration's job.

    ``reveals`` is a stated empty revelation and ``effects`` is a stated
    location, on purpose. An UNKNOWN revelation anywhere in a deferred
    closure REFUSES qualified publication, and this corpus is here to
    measure capacity, not to re-test the disclosure rules the E2 lane
    already exercises."""
    return {
        "id": action_id,
        "action_class": action_class,
        "covers": list(covers),
        "requires": list(requires),
        "conflicts": list(conflicts),
        "reveals": {"obligation_classes": [], "action_classes": []},
        "effects": {"locations": [f"loc:{action_id}"]},
        "duration": duration,
        "execution_cost": execution_cost,
        "cost_unit": COST_UNIT,
        "estimator": {"id": "e4-pinned-estimator", "version": 1},
    }


@dataclass(frozen=True)
class Workload:
    """One resource-free batch of work, plus the reason it is in the corpus.

    A workload is the thing held FIXED across the configuration grid.
    Its digest is a comparison-binding field: two arms compared at equal
    total capacity must be the same work, and
    :data:`COMPARISON_BINDING_FIELDS` says so."""

    name: str
    source: str
    exercises: str
    obligations: tuple[Mapping[str, Any], ...]
    actions: tuple[Mapping[str, Any], ...]

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source": self.source,
            "obligations": [dict(sorted(o.items()))
                            for o in sorted(self.obligations,
                                            key=lambda o: str(o["id"]))],
            "actions": [
                {k: (sorted(v.items()) if isinstance(v, Mapping) else v)
                 for k, v in sorted(a.items())}
                for a in sorted(self.actions, key=lambda a: str(a["id"]))
            ],
        }

    @property
    def digest(self) -> str:
        # The SAME local dialect the configuration digest uses, with the
        # same refusals. There is no ``default=str`` here and no second
        # canonicalization anywhere in this module (EWD-BRR-5).
        return _local_digest(self.to_canonical_dict())

    def action_ids(self) -> tuple[str, ...]:
        return tuple(str(a["id"]) for a in self.actions)


def mandatory_closure(workload: Workload) -> tuple[str, ...]:
    """The full prerequisite closure of every action covering a ``must``.

    This is the set §7.2 says a configured critical lane MUST admit, and
    §6 says is mandatory under the schedule-only ownership boundary. It
    is a structural property of the workload — reachability over
    ``requires`` — so deriving it here takes nothing away from the
    checker, which owns whether a PLAN discharges it."""
    must_ids = {
        str(o["id"]) for o in workload.obligations
        if o["priority"] == PRIORITY_MUST
    }
    by_id = {str(a["id"]): a for a in workload.actions}
    frontier = sorted(
        action_id for action_id, action in by_id.items()
        if must_ids & {str(c) for c in action["covers"]}
    )
    closure: set[str] = set()
    while frontier:
        action_id = frontier.pop()
        if action_id in closure:
            continue
        closure.add(action_id)
        frontier.extend(str(p) for p in by_id[action_id]["requires"])
    return tuple(sorted(closure))


def lane_assignment(
    workload: Workload, config: ResourceConfig
) -> Mapping[str, str]:
    """Which lane each action is eligible for under ``config``.

    Returns :data:`LANE_NOT_APPLICABLE` for every action of a
    configuration that declares no lanes. Nothing about this mapping
    reaches the schema: it decides which POOL each action's demand is
    written against, and then it is gone."""
    if config.lane_rule == LANE_RULE_NONE:
        return {a: LANE_NOT_APPLICABLE for a in workload.action_ids()}
    if config.lane_rule == LANE_RULE_MANDATORY_CLOSURE:
        critical = set(mandatory_closure(workload))
    elif config.lane_rule == LANE_RULE_ACTION_CLASS:
        critical = {
            str(a["id"]) for a in workload.actions
            if a["action_class"] == "repair"
        }
    else:
        raise BatchResourceReplayError(
            "unknown lane eligibility rule; the rule vocabulary is closed "
            "and an unmapped rule never defaults to admitting everything",
            data={"lane_rule": config.lane_rule},
        )
    return {
        action_id: (LANE_CRITICAL if action_id in critical else LANE_LIGHT)
        for action_id in workload.action_ids()
    }


#: Named result of the §7.2 validity gate on a compiled lane config.
LANE_CONFIG_VALID = "valid"
LANE_CONFIG_EXCLUDES_MANDATORY_CLOSURE = "excludes_mandatory_closure"


def lane_config_validity(
    workload: Workload, config: ResourceConfig
) -> tuple[str, tuple[str, ...]]:
    """§7.2's validity gate, evaluated BEFORE anything is planned.

    "If a critical lane is configured, it must admit the full
    prerequisite closure of mandatory actions. ... A configuration that
    starves a mandatory prerequisite is invalid." Returns the verdict and
    the mandatory-closure members the critical lane fails to admit."""
    if config.lane_rule == LANE_RULE_NONE:
        return LANE_CONFIG_VALID, ()
    lanes = lane_assignment(workload, config)
    excluded = tuple(sorted(
        action_id for action_id in mandatory_closure(workload)
        if lanes[action_id] != LANE_CRITICAL
    ))
    if excluded:
        return LANE_CONFIG_EXCLUDES_MANDATORY_CLOSURE, excluded
    return LANE_CONFIG_VALID, ()


def compile_instance(
    workload: Workload, config: ResourceConfig
) -> dict[str, Any]:
    """Compile one (workload, configuration) pair into a raw instance.

    This is where a lane stops being configuration and becomes named
    pools the frozen schema already expresses. Every action answers for
    EVERY declared pool — the zeros are written, not left to the loader's
    refusal — and the borrowing-ON arm adds the light-lane concurrency
    cap as a :class:`~...batch_schedule.ConflictGroup`."""
    lanes = lane_assignment(workload, config)
    pool_ids = sorted(config.pools)
    actions: list[dict[str, Any]] = []
    for raw in workload.actions:
        action = dict(raw)
        action_id = str(action["id"])
        if config.kind == KIND_HETEROGENEOUS:
            declared = HETEROGENEOUS_DEMAND.get(str(action["action_class"]))
            if declared is None:
                raise BatchResourceReplayError(
                    "the heterogeneous control has no declared demand for "
                    "this action class; an unmapped class is never a zero "
                    "demand (design §7)",
                    data={"action": action_id,
                          "action_class": action["action_class"]},
                )
            demand = {pool: int(declared[pool]) for pool in pool_ids}
        elif config.kind == KIND_LANE and config.borrowing == BORROWING_OFF:
            lane = lanes[action_id]
            demand = {pool: (1 if pool == lane else 0) for pool in pool_ids}
        else:
            # One pool: the shared arms, and the borrowing-ON lane arm
            # whose lane structure lives in the conflict group below.
            demand = {pool: 1 for pool in pool_ids}
        action["demand"] = demand
        actions.append(action)

    conflict_groups: list[dict[str, Any]] = []
    if config.kind == KIND_LANE and config.borrowing == BORROWING_ON_ASYMMETRIC:
        light_members = tuple(sorted(
            action_id for action_id, lane in lanes.items()
            if lane == LANE_LIGHT
        ))
        assert config.shares is not None
        light_share = config.shares[1]
        # A group constrains at least two actions (the schema says so),
        # and with fewer than two light actions the cap could not bind
        # anyway: one concurrent member never exceeds a share of one or
        # more. The omission is exact, not a rounding.
        if len(light_members) >= 2:
            conflict_groups.append({
                "id": "light_lane_share",
                "members": list(light_members),
                "max_concurrent": light_share,
            })

    scope = sorted({f"loc:{a['id']}" for a in actions} | {"loc:elsewhere"})
    resources = [
        {
            "id": pool,
            "version": 1,
            "segments": [dict(s) for s in config.pools[pool]],
            # A reservation and a hard cap that cannot bite: E4 measures
            # the instantaneous CAPACITY curve, and a consumable budget
            # that refused half the grid would be measuring a different
            # question. They are stated rather than UNKNOWN so the
            # checker can ENFORCE them instead of listing them as
            # not-established.
            "reservation": 10_000,
            "hard_cap": 99,
        }
        for pool in pool_ids
    ]
    return {
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "instance_id": f"e4-{workload.name}-{config.name}",
        "epoch": 0,
        "cost_unit": COST_UNIT,
        "unit_mapping_version": UNIT_MAPPING_VERSION,
        "changes": [{"id": "c1", "affected_scope": scope}],
        "obligations": [dict(o) for o in workload.obligations],
        "actions": actions,
        "conflict_groups": conflict_groups,
        "resources": resources,
    }


# --------------------------------------------------------------------------- #
# The hand-built workloads.                                                    #
# --------------------------------------------------------------------------- #
def _workload_mandatory_chain() -> Workload:
    return Workload(
        name="mandatory-chain-under-contention",
        source="hand_built",
        exercises=(
            "a two-step mandatory chain whose closure spans two action "
            "classes, beside three independent light actions. Under a "
            "small critical share the chain is the makespan; under a "
            "shared pool the light work competes with it"
        ),
        obligations=(
            _obligation("o_must", PRIORITY_MUST, "chain",
                        obligation_class="mandatory"),
            _obligation("o_should_a", PRIORITY_SHOULD, "light-a"),
            _obligation("o_should_b", PRIORITY_SHOULD, "light-b"),
            _obligation("o_may", PRIORITY_MAY, "light-c"),
        ),
        actions=(
            _work("m_probe", action_class="probe", duration=2,
                  execution_cost=3),
            _work("m_repair", action_class="repair", requires=["m_probe"],
                  covers=["o_must"], duration=2, execution_cost=4),
            _work("l_one", covers=["o_should_a"], duration=2,
                  execution_cost=1),
            _work("l_two", covers=["o_should_b"], duration=2,
                  execution_cost=1),
            _work("l_three", covers=["o_may"], duration=2, execution_cost=1),
        ),
    )


def _workload_parallel_mandatory() -> Workload:
    return Workload(
        name="parallel-mandatory-work",
        source="hand_built",
        exercises=(
            "three INDEPENDENT mandatory actions and two light ones. The "
            "mandatory set is wider than every critical share in the grid, "
            "so a split that walls it off pays for the wall in makespan "
            "while a shared pool of the same total does not"
        ),
        obligations=(
            _obligation("o_must_a", PRIORITY_MUST, "par-a",
                        obligation_class="mandatory"),
            _obligation("o_must_b", PRIORITY_MUST, "par-b",
                        obligation_class="mandatory"),
            _obligation("o_must_c", PRIORITY_MUST, "par-c",
                        obligation_class="mandatory"),
            _obligation("o_should", PRIORITY_SHOULD, "par-should"),
            _obligation("o_may", PRIORITY_MAY, "par-may"),
        ),
        actions=(
            _work("m_a", action_class="repair", covers=["o_must_a"],
                  duration=2, execution_cost=2),
            _work("m_b", action_class="repair", covers=["o_must_b"],
                  duration=2, execution_cost=2),
            _work("m_c", action_class="check", covers=["o_must_c"],
                  duration=2, execution_cost=2),
            _work("l_a", covers=["o_should"], duration=1, execution_cost=1),
            _work("l_b", covers=["o_may"], duration=1, execution_cost=1),
        ),
    )


def _workload_light_heavy() -> Workload:
    return Workload(
        name="light-heavy-one-mandatory",
        source="hand_built",
        exercises=(
            "one short mandatory action against four long light ones. The "
            "critical share is idle for most of the schedule, which is the "
            "case a lane split is supposed to be WORST at and the reason "
            "the comparison is capacity-matched rather than absolute"
        ),
        obligations=(
            _obligation("o_must", PRIORITY_MUST, "lh-must",
                        obligation_class="mandatory"),
            _obligation("o_should_a", PRIORITY_SHOULD, "lh-a"),
            _obligation("o_should_b", PRIORITY_SHOULD, "lh-b"),
            _obligation("o_may_a", PRIORITY_MAY, "lh-c"),
            _obligation("o_may_b", PRIORITY_MAY, "lh-d"),
        ),
        actions=(
            _work("m_only", action_class="repair", covers=["o_must"],
                  duration=1, execution_cost=1),
            _work("l_a", covers=["o_should_a"], duration=3, execution_cost=2),
            _work("l_b", covers=["o_should_b"], duration=3, execution_cost=2),
            _work("l_c", covers=["o_may_a"], duration=3, execution_cost=2),
            _work("l_d", covers=["o_may_b"], duration=3, execution_cost=2),
        ),
    )


def _workload_gated_closure() -> Workload:
    return Workload(
        name="gated-mandatory-closure",
        source="hand_built",
        exercises=(
            "a terminal publication gate whose prerequisite closure pulls "
            "in a check AND a repair, so the mandatory closure spans three "
            "action classes. A class-based lane rule splits that closure — "
            "the configuration §7.2 calls invalid — while the reviewed "
            "closure rule keeps it whole"
        ),
        obligations=(
            _obligation("o_must_gate", PRIORITY_MUST, "gate",
                        obligation_class="mandatory"),
            _obligation("o_should", PRIORITY_SHOULD, "gate-should"),
            _obligation("o_may", PRIORITY_MAY, "gate-may"),
        ),
        actions=(
            _work("g_check", action_class="check", duration=1,
                  execution_cost=1),
            _work("g_repair", action_class="repair", requires=["g_check"],
                  duration=2, execution_cost=3),
            _work("g_gate", action_class="gate", requires=["g_repair"],
                  covers=["o_must_gate"], duration=1, execution_cost=1),
            _work("s_one", covers=["o_should"], duration=2, execution_cost=1),
            _work("y_one", covers=["o_may"], duration=2, execution_cost=1),
        ),
    )


def hand_built_workloads() -> tuple[Workload, ...]:
    """The hand-built E4 workloads, in canonical order."""
    return (
        _workload_mandatory_chain(),
        _workload_parallel_mandatory(),
        _workload_light_heavy(),
        _workload_gated_closure(),
    )


#: Pinned into the manifest so a workload cannot be quietly added or
#: dropped between runs.
HAND_BUILT_WORKLOADS: tuple[str, ...] = tuple(
    w.name for w in hand_built_workloads())

#: The declared size distribution of the synthetic generator.
SYNTHETIC_ACTION_SIZES: tuple[int, ...] = (5, 6)


def synthetic_workload(seed: int) -> Workload:
    """One seeded synthetic workload. Every draw is PRF-keyed.

    Declared distribution: action count uniform over
    :data:`SYNTHETIC_ACTION_SIZES`; the first two actions form a
    mandatory chain (a ``probe`` and the ``repair`` that requires it and
    covers the ``must``), which guarantees a closure spanning two classes
    and at least one prerequisite edge inside the critical lane; every
    remaining action is light, covers a distinct ``should`` or ``may``
    unit, and takes a duration uniform in [1, 3] and a cost uniform in
    [1, 5]. No draw touches demand or capacity — those are the
    CONFIGURATION's, and a workload whose shape moved with the pool it
    ran on would make the capacity-matched comparison meaningless."""
    prf = SynthPrf(f"e4-synth:{seed}")
    count = prf.choice(SYNTHETIC_ACTION_SIZES, "size")
    obligations = [
        _obligation("o_must", PRIORITY_MUST, f"synth-must-{seed}",
                    obligation_class="mandatory"),
    ]
    actions = [
        _work("m_probe", action_class="probe",
              duration=prf.randint(1, 3, "duration", "m_probe"),
              execution_cost=prf.randint(1, 5, "cost", "m_probe")),
        _work("m_repair", action_class="repair", requires=["m_probe"],
              covers=["o_must"],
              duration=prf.randint(1, 3, "duration", "m_repair"),
              execution_cost=prf.randint(1, 5, "cost", "m_repair")),
    ]
    for index in range(count - 2):
        action_id = f"l_{index}"
        priority = (PRIORITY_SHOULD if index % 2 == 0 else PRIORITY_MAY)
        obligation_id = f"o_{priority}_{index}"
        obligations.append(
            _obligation(obligation_id, priority, f"synth-{seed}-{index}"))
        actions.append(_work(
            action_id,
            action_class=prf.choice(("check", "probe"), "class", action_id),
            covers=[obligation_id],
            duration=prf.randint(1, 3, "duration", action_id),
            execution_cost=prf.randint(1, 5, "cost", action_id),
        ))
    return Workload(
        name=f"synthetic-{seed}",
        source="synthetic",
        exercises=(
            "a seeded synthetic workload from the declared size "
            "distribution; the grid measures whether the hand-built "
            "conclusions survive shapes nobody chose by hand"
        ),
        obligations=tuple(obligations),
        actions=tuple(actions),
    )


def workloads(manifest: "E4Manifest") -> tuple[Workload, ...]:
    """The manifest's whole corpus: hand-built workloads then the grid."""
    built = {w.name: w for w in hand_built_workloads()}
    missing = [name for name in manifest.hand_built if name not in built]
    if missing:
        raise BatchResourceReplayError(
            "the manifest names a hand-built workload the corpus does not "
            "define; a missing workload is never a silently smaller run",
            data={"missing": missing},
        )
    return tuple(
        [built[name] for name in manifest.hand_built]
        + [synthetic_workload(seed) for seed in manifest.synthetic_seeds]
    )


# --------------------------------------------------------------------------- #
# The preregistered manifest.                                                  #
# --------------------------------------------------------------------------- #
DEFAULT_SYNTHETIC_SEEDS: tuple[int, ...] = (211, 223)

#: The preregistered lane threshold, fixed BEFORE any outcome was read: a
#: lane split earns the hardcoding claim only if it strictly beats its
#: capacity-matched shared pool on at least this fraction of the
#: (workload, candidate) pairs where both arms are checkable.
DEFAULT_LANE_THRESHOLD = Fraction(1, 2)

#: §7.2 offers no starvation budget — "a configuration that starves a
#: mandatory prerequisite is invalid" — so the threshold is zero starved
#: mandatory prerequisites, not a delay allowance.
STARVATION_THRESHOLD = 0

#: The smallest comparable-pair denominator the preregistered comparison
#: will draw a lane conclusion over. Fixed structurally, before any
#: outcome was read: the roster is exhaustive per workload (5 candidates),
#: and a claim about "the corpus" drawn from fewer than three of its six
#: workloads is a claim about a shape, not about the corpus. 3 x 5 = 15.
#: Below it — and at zero, where "measured nothing" and "measured and
#: lost" would otherwise be the same number — the comparison REFUSES to
#: conclude (EWD-BRR-2).
MINIMUM_COMPARABLE_PAIRS = 15

CLAUSE_LANE_EXCLUDES_MANDATORY_CLOSURE = "lane_config_excludes_mandatory_closure"
CLAUSE_MANDATORY_PREREQUISITE_STARVED = (
    "mandatory_prerequisite_single_move_starved")
CLAUSE_MANDATORY_PREREQUISITE_INVERSION = (
    "mandatory_prerequisite_inversion_starved")
CLAUSE_REALIZED_CAPACITY_VIOLATION = "realized_capacity_violation"
CLAUSE_CHECKER_REFUSAL_READ_AS_PASS = "checker_refusal_read_as_pass"
CLAUSE_CAPACITY_MATCHED_COMPARISON = "capacity_matched_comparison_not_beaten"

#: Every falsification clause this lane declares, with the text used to
#: report it. A clause is reported whether it fires or not — a falsifier
#: nobody can see is not a falsifier — and each text states the WHOLE
#: preregistered proposition it is evaluated against, with any deferred
#: term marked INSIDE the string rather than only in another object.
CLAUSES: Mapping[str, str] = {
    CLAUSE_LANE_EXCLUDES_MANDATORY_CLOSURE: (
        "E4/§7.2: a lane configuration starves a mandatory prerequisite by "
        "CONFIGURATION. Evaluated statically, before anything is planned, "
        "over every (workload, lane configuration) pair in the grid: §7.2 "
        "says 'if a critical lane is configured, it must admit the full "
        "prerequisite closure of mandatory actions' and 'a configuration "
        "that starves a mandatory prerequisite is invalid'. The clause "
        "fires when any member of the mandatory closure — the transitive "
        "prerequisite closure of every action covering a `must` obligation "
        "— is assigned to a lane other than critical"
    ),
    CLAUSE_MANDATORY_PREREQUISITE_STARVED: (
        "E4/§7.2: a configuration starves a mandatory prerequisite in the "
        "SCHEDULE by leaving it waiting on IDLE capacity. Evaluated over "
        "every checked-feasible plan: a mandatory-closure action is "
        "SINGLE-MOVE starved when it waited (start > the latest end among "
        "its prerequisites) while room existed for it — formally, when "
        "some integer t in [ready, start) yields a plan, identical but for "
        "moving that ONE action to t, that the INDEPENDENT CHECKER "
        "ACCEPTS. The checker decides 'room existed', so capacity, hard "
        "caps, conflicts, groups, and prerequisites mean exactly what they "
        "already mean. This clause is SINGLE-MOVE by name and by "
        "construction: it never displaces lower-priority work, so under a "
        "work-conserving planner it cannot see a mandatory action that "
        "waited BEHIND lower-priority work [DEFERRED: "
        "starvation_probe_is_single_move_only]. That phenomenon is the "
        "separate mandatory_prerequisite_inversion_starved clause. "
        "Threshold: ZERO — §7.2 offers no budget"
    ),
    CLAUSE_MANDATORY_PREREQUISITE_INVERSION: (
        "E4/§7.2: a LANE configuration starves a mandatory prerequisite by "
        "PRIORITY INVERSION. Evaluated over every checked-feasible plan: a "
        "mandatory-closure action is inversion-starved when it waited "
        "while a strictly-lower-priority action — one outside the "
        "mandatory closure — was running in the interval it waited "
        "through, AND the displaced plan (that action moved to the "
        "occupied time t, the lower-priority work it displaces delayed by "
        "its duration) is ACCEPTED by the INDEPENDENT CHECKER. The checker "
        "decides the counterfactual; the displacement rule is a "
        "sufficient construction and undercounts rather than guesses "
        "[DEFERRED: inversion_probe_displacement_rule]. SCOPE, from §7.2's "
        "own sentence: the clause is a validity failure ONLY for a "
        "configuration that CONFIGURES a critical lane. §7.2 conditions "
        "the promise — 'IF a critical lane is configured, it must admit "
        "the full prerequisite closure of mandatory actions' — and a "
        "generic shared pool configures no lane and makes no such "
        "promise, so an inversion under a shared pool is a MEASUREMENT "
        "reported against that pool, not an invalid configuration. Under a "
        "lane configuration the promise is live and the threshold is "
        "ZERO: a critical lane that admits the closure on paper and then "
        "lets light work hold the capacity has starved it in fact"
    ),
    CLAUSE_REALIZED_CAPACITY_VIOLATION: (
        "E4: a lane configuration violates a named capacity. Evaluated "
        "over every plan the harness submits — accepted or refused — by "
        "reading the checker's own causes for capacity_exceeded, "
        "hard_cap_exceeded, or reservation_exceeded [DEFERRED: "
        "realized_execution_measurements — nothing is EXECUTED here, so "
        "these are PLANNED capacities validated by the checker, not "
        "realized ones; and DEFERRED: capacity_violation_event_"
        "multiplicity — the checker reports at most one capacity finding "
        "per resource, so a firing counts resources-with-violation, not "
        "events]. Zero tolerance: a candidate whose plan the checker "
        "refuses for a capacity cause is an answer, and a candidate whose "
        "ACCEPTED plan carried one would be a contradiction"
    ),
    CLAUSE_CHECKER_REFUSAL_READ_AS_PASS: (
        "E4: a checker refusal read as a pass. Evaluated structurally over "
        "every reported row: a row may report qualification only when the "
        "checker's own verdict was checked_feasible with no findings, and "
        "a row that reports an objective tuple while the checker refused "
        "would be this harness overriding the only validity authority it "
        "has. Zero tolerance"
    ),
    CLAUSE_CAPACITY_MATCHED_COMPARISON: (
        "E4: a lane configuration fails its preregistered CAPACITY-MATCHED "
        "comparison with the generic shared pool, under the declared "
        "seeds, primary metric, aggregation, uncertainty method, and "
        "threshold. Each lane split (c, l) is paired with the shared pool "
        "at capacity c+l — equal TOTAL capacity, so the comparison is "
        "about the partition and not about having more machine. A pair is "
        "compared only when both arms bind the same workload digest, "
        "candidate, roster version, and manifest digest. In that case, per "
        "the design's own sentence, lanes remain an experiment "
        "configuration and are NOT hardcoded"
    ),
}

#: The zero-tolerance subset (§11 preamble). Any one firing refutes the
#: slice; the comparison clause rejects the LANE claim without refuting
#: the mechanism, which is exactly what §11 E4's last sentence says
#: should then happen.
ZERO_TOLERANCE: tuple[str, ...] = (
    CLAUSE_CHECKER_REFUSAL_READ_AS_PASS,
    CLAUSE_LANE_EXCLUDES_MANDATORY_CLOSURE,
    CLAUSE_MANDATORY_PREREQUISITE_INVERSION,
    CLAUSE_MANDATORY_PREREQUISITE_STARVED,
    CLAUSE_REALIZED_CAPACITY_VIOLATION,
)

#: The fields two arms must AGREE on before their objective tuples may be
#: subtracted from one another. A per-arm sanity check does not catch a
#: cross-arm mismatch, so the comparison refuses the pair by name instead
#: of quietly comparing two different experiments.
COMPARISON_BINDING_FIELDS: tuple[str, ...] = (
    "candidate",
    "manifest_digest",
    "roster_version",
    "workload_digest",
)

#: The named deferrals. Each one is carried in the manifest and echoed in
#: the report: a deferral is a row, never a silent cap.
DEFERRED: Mapping[str, str] = {
    "symmetric_capacity_borrowing": (
        "§11 E4 asks for the split configurations 'with borrowing disabled "
        "and enabled'. The ASYMMETRIC policy — a critical action may draw "
        "on idle light capacity, the light lane may never exceed its "
        "share — IS expressible in the frozen schema and RUNS here: one "
        "pool at the summed capacity plus a ConflictGroup over the "
        "light-lane actions at max_concurrent = the light share. SYMMETRIC "
        "borrowing is DEFERRED, and the gap is precise: the schema fixes "
        "demand(a, r) as a CONSTANT per (action, resource) pair and offers "
        "no disjunctive or state-dependent demand, so 'draw from whichever "
        "pool has room' has nowhere to live; and a combined pool with no "
        "cap at all does not express it either, because that IS the shared "
        "pool. Expressing it would need a schema change, which this task "
        "does not own"
    ),
    "preemption": (
        "§7 makes actions non-preemptive unless the instance decomposes "
        "them into explicit execution chunks with their own identities, "
        "dependencies, effects, and demands, and §11 E4 declares no "
        "preemption policy. Nothing here preempts. The chunk decomposition "
        "the schema does offer is DEFERRED rather than approximated: a "
        "half-modelled preemption would change what every makespan and "
        "utilisation number in this report means"
    ),
    "realized_execution_measurements": (
        "§7.1 separates estimated, reserved, and REALIZED quantities. "
        "Nothing is executed here, so every capacity, utilisation, delay, "
        "and makespan number is over PINNED estimates validated by the "
        "checker. 'Realized capacity violations' is therefore evaluated as "
        "'capacity violations in a checked plan'. The realized half "
        "belongs to the measurement adapter's live path"
    ),
    "history_window_batches": (
        "E4 replays configurations over batches; the deterministic "
        "22-update histories replayed in windows of 1, 4, and 8 need the "
        "database-backed corpus-to-instance binding, which does not exist "
        "yet. DEFERRED, exactly as the E2 lane defers it; the hand-built "
        "workloads and the seeded synthetic grid are the data actually used"
    ),
    "unit_demand_lane_compilation": (
        "The borrowing-ON compilation states the light lane's share as a "
        "COUNT of concurrent group members, which is exact only while a "
        "lane-arm action demands ONE unit of its pool. Every lane arm in "
        "this grid therefore uses unit demand, and the heterogeneous "
        "control — which has no lanes — carries the genuine unequal "
        "per-pool demands. Multi-unit demands under borrowing are "
        "DEFERRED: a group cap cannot express 'at most l UNITS' when "
        "members differ in size, and inventing a per-unit decomposition "
        "would change the action identities the objective counts"
    ),
    "capacity_violation_event_multiplicity": (
        "The checker reports at most one capacity_exceeded finding per "
        "resource and stops that resource's event scan at a hard-cap "
        "finding (recorded as EWD-BSC-9, MINOR, not addressed). The "
        "violation count reported here is therefore "
        "RESOURCES-WITH-A-VIOLATION, not violation EVENTS. The "
        "zero-tolerance clause is unaffected — zero is zero either way — "
        "but no reader should read the count as an event tally"
    ),
    "queue_delay_against_release_time": (
        "Queue delay is measured from DEPENDENCY READINESS, not from an "
        "arrival or release time, because the frozen instance declares "
        "none: §7's action carries prerequisites, durations, costs, and "
        "demands, and no release time. A queue delay against arrival is "
        "DEFERRED until the instance can express one"
    ),
    "starvation_probe_is_single_move_only": (
        "The single-move starvation probe moves the WAITING mandatory "
        "action and nothing else, so it can only find capacity that was "
        "already IDLE. Under a work-conserving planner it therefore cannot "
        "observe a mandatory action that waited BEHIND lower-priority "
        "work, which is the phenomenon §7.2's sentence is about. The "
        "locality is a bound on that metric, and it is why the metric is "
        "named single_move_starvation rather than starvation. The missing "
        "half is not deferred: it is the separate inversion-starvation "
        "metric and its own zero-tolerance clause"
    ),
    "inversion_probe_displacement_rule": (
        "The inversion probe's counterfactual uses ONE fixed displacement "
        "rule — the mandatory action takes the occupied time t, and every "
        "strictly-lower-priority action starting at or after t is delayed "
        "by that action's duration — and asks the checker. It is a "
        "SUFFICIENT construction, not an exhaustive search over "
        "rearrangements: an inversion whose improved placement needs some "
        "other reordering is NOT counted. The error is one-directional — "
        "the probe undercounts, never overcounts — so a firing is real and "
        "a zero is 'none under this rule', not 'none'. An exhaustive "
        "counterfactual is a search problem this task does not own"
    ),
    "optimality_reference": (
        "No exhaustive-oracle arm runs here. §11 E4's metric list names "
        "no gap-to-optimum, and §9.1 permits the word 'optimal' only "
        "through a checked artifact. Every number in this report is a "
        "CHECKED FEASIBLE candidate's checked value; the oracle reference "
        "belongs to task.batch.candidate-planner, where it runs"
    ),
}

#: The deferrals that make this run PARTIAL E4 rather than E4. Each one
#: subtracts from the experiment §11 E4 declares — an arm of the
#: borrowing clause, the data clause, the word 'realized' in the metric
#: list, or a bound on the compilation the grid runs through — so a clear
#: run of what DID execute is not the sentence "E4 passed".
PARTIAL_E4_DEFERRALS: tuple[str, ...] = (
    "history_window_batches",
    "realized_execution_measurements",
    "symmetric_capacity_borrowing",
    "unit_demand_lane_compilation",
)

VERDICT_PASS = "pass"
VERDICT_PASS_PARTIAL_E4 = "pass_partial_e4"

#: The two derived conclusions on the design's lane question. DERIVED
#: from the preregistered comparison, never authored: §11 E4's own
#: sentence is that a failed comparison leaves lanes an experiment
#: configuration, not a runtime surface.
CONCLUSION_NOT_HARDCODED = "lanes_remain_experiment_configuration_not_hardcoded"
CONCLUSION_HARDCODING_SUPPORTED = "lanes_meet_preregistered_hardcoding_threshold"

#: The THIRD conclusion, and the reason there has to be one: a comparison
#: over zero comparable pairs is not a negative result about lanes, it is
#: the absence of a measurement, and publishing the negative sentence over
#: it would be an absence dressed as an answer (EWD-BRR-2). It is also
#: what a below-minimum denominator gets: see
#: :data:`MINIMUM_COMPARABLE_PAIRS`.
CONCLUSION_NOT_ESTABLISHED = "not_established:no_comparable_pairs"
#: And a conclusion may not be published at all beneath an ``invalid:``
#: verdict, whatever the denominator was.
CONCLUSION_NOT_ESTABLISHED_INVALID = "not_established:report_invalid"

#: The suffix a derived verdict carries when any grid row REFUSED. A
#: shrunken experiment must be loud in the verdict, not inferable from a
#: denominator (EWD-BRR-2).
VERDICT_REFUSALS_SUFFIX = "with_refusals"


@dataclass(frozen=True)
class E4Manifest:
    """The preregistered E4 manifest, frozen before outcomes.

    Built by :func:`build_manifest` from CONFIGURATION ONLY. It takes no
    result object of any kind, so a manifest cannot be shaped by what the
    run produced; :meth:`digest` binds every report row."""

    hand_built: tuple[str, ...]
    synthetic_seeds: tuple[int, ...]
    synthetic_action_sizes: tuple[int, ...]
    configurations: tuple[Mapping[str, Any], ...]
    design_named_configurations: tuple[str, ...]
    candidates: tuple[str, ...]
    roster_version: str
    capacity_matched_pairs: Mapping[str, str]
    comparison_binding_fields: tuple[str, ...]
    lane_compilation: Mapping[str, str]
    metric_definitions: Mapping[str, str]
    #: Metrics that are REPORTED and compared but decide nothing. Named in
    #: the preregistration so a reader can tell a secondary surface from
    #: the decider without reading ``primary_metric`` first (EWD-BRR-8).
    secondary_metrics: Mapping[str, str]
    starvation_threshold: int
    #: The preregistered floor on the comparable-pair denominator below
    #: which no lane conclusion is drawn (EWD-BRR-2).
    minimum_comparable_pairs: int
    zero_tolerance: Mapping[str, str]
    falsification_clauses: Mapping[str, str]
    primary_metric: str
    comparator: str
    comparator_threshold: str
    aggregation: str
    uncertainty: str
    threshold: str
    deferred: Mapping[str, str]
    schema: str = "okg.synth.e4-resource-replay.v1"

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "hand_built": list(self.hand_built),
            "synthetic_seeds": list(self.synthetic_seeds),
            "synthetic_action_sizes": list(self.synthetic_action_sizes),
            "configurations": [dict(sorted(c.items()))
                               for c in self.configurations],
            "design_named_configurations": list(
                self.design_named_configurations),
            "candidates": list(self.candidates),
            "roster_version": self.roster_version,
            "capacity_matched_pairs": dict(
                sorted(self.capacity_matched_pairs.items())),
            "comparison_binding_fields": list(self.comparison_binding_fields),
            "lane_compilation": dict(sorted(self.lane_compilation.items())),
            "metric_definitions": dict(sorted(self.metric_definitions.items())),
            "secondary_metrics": dict(sorted(self.secondary_metrics.items())),
            "starvation_threshold": self.starvation_threshold,
            "minimum_comparable_pairs": self.minimum_comparable_pairs,
            "zero_tolerance": dict(sorted(self.zero_tolerance.items())),
            "falsification_clauses": dict(
                sorted(self.falsification_clauses.items())),
            "primary_metric": self.primary_metric,
            "comparator": self.comparator,
            "comparator_threshold": self.comparator_threshold,
            "aggregation": self.aggregation,
            "uncertainty": self.uncertainty,
            "threshold": self.threshold,
            "deferred": dict(sorted(self.deferred.items())),
        }

    def canonical_json(self) -> str:
        return json.dumps(
            self.to_canonical_dict(), sort_keys=True, separators=(",", ":")
        )

    def digest(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


#: How each lane split is paired with a shared pool at EQUAL total
#: capacity. Derived from the grid, not authored, so a new split cannot
#: acquire a partner by hand.
def capacity_matched_pairs() -> Mapping[str, str]:
    shared = {
        config.total_capacity: config.name
        for config in configurations() if config.kind == KIND_SHARED
    }
    pairs: dict[str, str] = {}
    for config in configurations():
        if config.kind != KIND_LANE:
            continue
        partner = shared.get(config.total_capacity)
        if partner is None:
            raise BatchResourceReplayError(
                "a lane split has no shared pool at its total capacity; the "
                "capacity-MATCHED comparison would have to compare unequal "
                "machines, and it refuses instead",
                data={"config": config.name,
                      "total_capacity": config.total_capacity},
            )
        pairs[config.name] = partner
    return pairs


def build_manifest(
    *,
    hand_built: Sequence[str] = HAND_BUILT_WORKLOADS,
    synthetic_seeds: Sequence[int] = DEFAULT_SYNTHETIC_SEEDS,
    lane_threshold: Fraction = DEFAULT_LANE_THRESHOLD,
) -> E4Manifest:
    """The one manifest constructor: configuration in, preregistration out."""
    if not hand_built:
        raise BatchResourceReplayError(
            "E4 needs its hand-built workloads; a run over synthetic "
            "workloads alone tests no shape anybody chose",
            data={"hand_built": list(hand_built)},
        )
    return E4Manifest(
        hand_built=tuple(hand_built),
        synthetic_seeds=tuple(int(s) for s in synthetic_seeds),
        synthetic_action_sizes=SYNTHETIC_ACTION_SIZES,
        configurations=tuple(
            c.to_canonical_dict() for c in configurations()),
        design_named_configurations=DESIGN_NAMED_CONFIGURATIONS,
        candidates=tuple(name for name, _ in CANDIDATES),
        roster_version=REPLAY_ROSTER_VERSION,
        capacity_matched_pairs=capacity_matched_pairs(),
        comparison_binding_fields=COMPARISON_BINDING_FIELDS,
        lane_compilation={
            "eligibility": (
                "critical-eligible is the full prerequisite closure of "
                "every action covering a `must` obligation (§7.2's 'must "
                "admit the full prerequisite closure of mandatory "
                "actions'); every other action is light. The rule is "
                "checked, not trusted: a configuration that leaves any "
                "closure member outside critical is graded INVALID"
            ),
            "borrowing_off": (
                "two named pools, `critical` at the critical share and "
                "`light` at the light share; each action's demand is "
                "written as 1 on its eligible pool and an EXPLICIT 0 on "
                "the other"
            ),
            "borrowing_on": (
                "one named pool, `lane_pool`, at the SUM of the shares, "
                "plus a ConflictGroup `light_lane_share` over the "
                "light-lane actions at max_concurrent = the light share. A "
                "critical action may therefore draw on idle light "
                "capacity; the light lane may never exceed its own share. "
                "The group is omitted only when the workload has fewer "
                "than two light actions, where a cap of one or more could "
                "not bind"
            ),
            "no_schema_change": (
                "nothing above adds a field, a lane kind, or an "
                "eligibility relation to the frozen schema; the checker "
                "validates every compiled configuration as ordinary named "
                "pools and conflict groups"
            ),
        },
        metric_definitions={
            "qualification": (
                "the INDEPENDENT CHECKER's verdict: checked_feasible with "
                "no findings. Never re-derived here"
            ),
            "objective_tuple": (
                "the §8 tuple as RECOMPUTED by the checker. The planner's "
                "claimed value is never the metric"
            ),
            "makespan": "stage 4 of the checker's recomputed tuple",
            "utilization": (
                "per pool: (sum over selected actions of demand x duration) "
                "/ (sum over integer times t in [0, makespan) of the "
                "declared capacity at t), reported UNREDUCED as "
                "'consumed/available'. An UNKNOWN capacity anywhere in the "
                "window reports 'unknown', never a number; a zero "
                "denominator reports 'not_applicable:empty_window', never "
                "a zero"
            ),
            "queue_delay": (
                "per selected action: start - ready, where ready is the "
                "latest end among its prerequisites in the same plan and 0 "
                "for an action with none. Reported as a total and a "
                "maximum over the plan"
            ),
            "mandatory_prerequisite_delay": (
                "the same quantity restricted to the mandatory closure"
            ),
            "single_move_starvation": (
                "a mandatory-closure action is SINGLE-MOVE STARVED when "
                "some integer t in [ready, start) yields a plan, identical "
                "but for moving that ONE action to t, that the INDEPENDENT "
                "CHECKER ACCEPTS. Not a threshold on delay: a delay can be "
                "honest work-conservation, and the checker — not this "
                "harness — decides whether room existed. The name says the "
                "bound: it moves nothing else, so it sees IDLE capacity "
                "only. See deferred.starvation_probe_is_single_move_only"
            ),
            "inversion_starvation": (
                "a mandatory-closure action is INVERSION-STARVED when it "
                "waited while a strictly-lower-priority action (one "
                "outside the mandatory closure) was running in the "
                "interval it waited through, and the DISPLACED plan — that "
                "action moved to the occupied time t, every "
                "strictly-lower-priority action starting at or after t "
                "delayed by its duration — is ACCEPTED by the INDEPENDENT "
                "CHECKER. This is the phenomenon §7.2's sentence names and "
                "the single-move probe cannot observe. See "
                "deferred.inversion_probe_displacement_rule for the "
                "one-directional undercount"
            ),
            "realized_capacity_violations": (
                "checker causes capacity_exceeded, hard_cap_exceeded, and "
                "reservation_exceeded over every submitted plan. See "
                "deferred.realized_execution_measurements and "
                "deferred.capacity_violation_event_multiplicity"
            ),
        },
        secondary_metrics={
            "mandatory_prerequisite_delay_paired_comparison": (
                "§11 E4's metric list names mandatory prerequisite delay. "
                "The same paired, capacity-matched comparison the primary "
                "metric uses is ALSO run over each row's "
                "mandatory_delay_total and reported per configuration "
                "(better/tied/worse, lower is better). It decides NOTHING: "
                "it does not enter `met`, the falsification clause, the "
                "verdict, or the lanes conclusion. It is reported because "
                "a §11 E4 metric that is collected and never compared is a "
                "signal the report drops on the floor"
            ),
            "inversion_starvation_under_a_shared_pool": (
                "Inversion-starvation counted against a SHARED or "
                "HETEROGENEOUS configuration is reported and decides "
                "nothing: §7.2 conditions the lane promise on a critical "
                "lane being CONFIGURED, and a generic pool configures "
                "none. Only the lane-configuration rows are "
                "validity-bearing, under the zero-tolerance clause"
            ),
        },
        starvation_threshold=STARVATION_THRESHOLD,
        minimum_comparable_pairs=MINIMUM_COMPARABLE_PAIRS,
        zero_tolerance={name: CLAUSES[name] for name in ZERO_TOLERANCE},
        falsification_clauses=dict(CLAUSES),
        primary_metric=(
            "stages 1-4 of the §8 lexicographic objective tuple as "
            "RECOMPUTED by the independent checker: (distinct should units "
            "desc, distinct may units desc, pinned total cost asc, pinned "
            "makespan asc). Stage 5 is EXCLUDED from the comparison and "
            "only from the comparison: it is a canonical serialization of "
            "the plan, and two arms of a capacity-matched pair have "
            "different instance ids and different pool ids by "
            "construction, so stage 5 would decide every comparison on a "
            "difference that is not about capacity"
        ),
        comparator=(
            "the generic shared pool at the lane split's TOTAL capacity "
            "(see capacity_matched_pairs). A lane split 'beats' it on a "
            "(workload, candidate) pair when its checked stages 1-4 sort "
            "strictly ahead under the §8 ordering"
        ),
        comparator_threshold=(
            f"{lane_threshold.numerator}/{lane_threshold.denominator}"
        ),
        aggregation=(
            "per (workload, candidate) exact comparison inside each "
            "lane-configuration / shared-pool pair, over the whole corpus; "
            "the deterministic candidate roster is exhaustive per workload "
            "so there is no sampling to aggregate over"
        ),
        uncertainty=(
            "deterministic exhaustive counts — no sampling, no confidence "
            "interval. Fractions are reported UNREDUCED so the evidence "
            "behind them is visible. A pair whose arms disagree on any "
            "comparison-binding field is reported as not compared, never "
            "as a tie"
        ),
        threshold=(
            "every zero-tolerance clause at zero, AND — for the lane "
            "hardcoding conclusion specifically — some lane configuration "
            "strictly beating its capacity-matched shared pool on at least "
            f"{lane_threshold.numerator}/{lane_threshold.denominator} of "
            "the (workload, candidate) pairs where both arms are "
            "checkable. Falling short does not refute the mechanism: per "
            "§11 E4 it leaves lanes an experiment configuration that is "
            "NOT hardcoded. That negative sentence is earned only over a "
            f"denominator of at least {MINIMUM_COMPARABLE_PAIRS} "
            "comparable pairs (three of the six corpus workloads at the "
            "exhaustive five-candidate roster); at or below zero, or "
            "beneath that floor, the comparison reports "
            f"'{CONCLUSION_NOT_ESTABLISHED}' and concludes nothing"
        ),
        deferred=dict(DEFERRED),
    )


#: The PREREGISTERED manifest digest: the digest of ``build_manifest()``
#: under its default configuration, pinned so that an outcome-driven edit
#: to the corpus, the grid, the clauses, the metric definitions, the
#: comparator, or the threshold cannot pass silently — changing the
#: configuration means changing this constant in the same reviewable
#: diff. The witness asserts the equality.
PREREGISTERED_MANIFEST_DIGEST = (
    "2ff898845452a563cf9ea5480ce5a07be29eaf522b7573d8f07f1d8da753645e"
)


# --------------------------------------------------------------------------- #
# Running one candidate against the checker.                                   #
# --------------------------------------------------------------------------- #
def _producer(candidate: str) -> ProducerRef:
    """§12 producer identity, bound to E4's own roster."""
    return ProducerRef(
        family=PRODUCER_FAMILY,
        version=PRODUCER_VERSION,
        parameters_digest=object_digest(
            {"candidate": candidate, "roster": REPLAY_ROSTER_VERSION}),
        seed=Unknown(
            "every candidate on this roster is deterministic and draws no "
            "random value; the workload seeds are the corpus's, not the "
            "planner's"),
        termination_status=TERMINATION_EXHAUSTED,
    )


def _plan_candidate(instance: BatchInstance, candidate: str) -> SchedulePlan:
    for name, planner in CANDIDATES:
        if name == candidate:
            return planner(instance)
    raise BatchResourceReplayError(
        "unknown candidate; the roster is closed",
        data={"candidate": candidate},
    )


def _ready_time(instance: BatchInstance, plan: SchedulePlan,
                action_id: str) -> int:
    action = instance.action_by_id(action_id)
    assert action is not None
    by_id = {s.action_id: s for s in plan.selected}
    return max(
        (by_id[p].end for p in action.requires if p in by_id), default=0)


def _shifted_plan(
    plan: SchedulePlan, base: ObjectiveTuple, action_id: str, start: int
) -> SchedulePlan:
    """The same plan with ONE action moved earlier, objective rebuilt.

    Stages 1-3 come from the checker's own recomputation of the base
    plan: the selection, the witnesses, and the outcomes are untouched by
    a shift, so those three numbers are unchanged by construction and
    re-deriving them here would be this module recomputing an objective.
    Stages 4 and 5 are mechanical functions of the placement, and stage 5
    goes through the trusted base's own serializer."""
    moved = None
    selected = []
    for scheduled in plan.selected:
        if scheduled.action_id == action_id:
            moved = replace(scheduled, start=start,
                            end=start + (scheduled.end - scheduled.start))
            selected.append(moved)
        else:
            selected.append(scheduled)
    if moved is None:
        raise BatchResourceReplayError(
            "cannot shift an action the plan did not select",
            data={"action": action_id},
        )
    return SchedulePlan(
        schema_version=plan.schema_version,
        instance_id=plan.instance_id,
        selected=tuple(selected),
        deferred=plan.deferred,
        witnesses=plan.witnesses,
        may_change=plan.may_change,
        claimed_objective=ObjectiveTuple(
            should_units=base.should_units,
            may_units=base.may_units,
            total_cost=base.total_cost,
            makespan=max(s.end for s in selected),
            tie_break=canonical_schedule_serialization(
                selected, plan.deferred),
        ),
    )


def _accepted(instance: BatchInstance, plan: SchedulePlan,
              candidate: str) -> bool:
    """Does the INDEPENDENT CHECKER accept this plan? The only authority."""
    certificate = build_certificate(
        instance, plan, producer=_producer(candidate),
        optimality_artifact_digest=ARTIFACT_UNBOUND)
    return check_schedule(instance, plan, certificate).accepted


def single_move_starved_actions(
    instance: BatchInstance,
    plan: SchedulePlan,
    base: ObjectiveTuple,
    mandatory: Sequence[str],
    candidate: str,
) -> tuple[tuple[str, ...], int]:
    """Mandatory-closure actions the checker would have run earlier ALONE.

    The probe is SINGLE-MOVE by construction: it moves the waiting
    mandatory action and nothing else, so it can only find room that was
    already idle. It cannot see a mandatory action that waited because
    lower-priority work was holding the capacity — that is the separate
    :func:`inversion_starved_actions` probe, and the bound is a named
    deferral row (``starvation_probe_is_single_move_only``) rather than a
    silence.

    Returns the starved action ids and how many actions were actually
    PROBED. The probe count is reported: a starvation result over zero
    probes is not a clean run, it is a run that measured nothing, and the
    non-vacuity witness reads this number."""
    starved: list[str] = []
    probed = 0
    by_id = {s.action_id: s for s in plan.selected}
    for action_id in sorted(mandatory):
        scheduled = by_id.get(action_id)
        if scheduled is None:
            continue
        ready = _ready_time(instance, plan, action_id)
        if scheduled.start <= ready:
            continue
        probed += 1
        for start in range(ready, scheduled.start):
            shifted = _shifted_plan(plan, base, action_id, start)
            if _accepted(instance, shifted, candidate):
                starved.append(action_id)
                break
    return tuple(starved), probed


def _displaced_plan(
    plan: SchedulePlan,
    base: ObjectiveTuple,
    action_id: str,
    start: int,
    lower_priority: Sequence[str],
) -> SchedulePlan:
    """Move ONE mandatory action earlier and DELAY the work it displaces.

    The counterfactual a priority-inversion probe needs is not "was the
    slot idle" but "should the lower-priority work have waited instead".
    The displacement rule is fixed, deterministic, and search-free: the
    mandatory action takes ``start``, and every strictly-lower-priority
    action that begins at or after ``start`` is delayed by exactly the
    mandatory action's duration. Nothing is reordered among the displaced
    work, nothing is dropped, and no mandatory action moves except the one
    named — so the SELECTION is untouched and stages 1-3 of the objective
    are unchanged by construction, exactly as in :func:`_shifted_plan`.

    The rule is a sufficient construction, not a complete search: a
    workload where the improved placement needs some OTHER rearrangement
    goes unreported. That undercount is a named row
    (``inversion_probe_displacement_rule``), never a silent cap, and it
    can only make this probe report FEWER inversions than exist."""
    lower = set(lower_priority)
    moved = None
    duration = None
    for scheduled in plan.selected:
        if scheduled.action_id == action_id:
            duration = scheduled.end - scheduled.start
    if duration is None:
        raise BatchResourceReplayError(
            "cannot displace around an action the plan did not select",
            data={"action": action_id},
        )
    selected = []
    for scheduled in plan.selected:
        if scheduled.action_id == action_id:
            moved = replace(scheduled, start=start, end=start + duration)
            selected.append(moved)
        elif scheduled.action_id in lower and scheduled.start >= start:
            selected.append(replace(
                scheduled, start=scheduled.start + duration,
                end=scheduled.end + duration))
        else:
            selected.append(scheduled)
    assert moved is not None
    return SchedulePlan(
        schema_version=plan.schema_version,
        instance_id=plan.instance_id,
        selected=tuple(selected),
        deferred=plan.deferred,
        witnesses=plan.witnesses,
        may_change=plan.may_change,
        claimed_objective=ObjectiveTuple(
            should_units=base.should_units,
            may_units=base.may_units,
            total_cost=base.total_cost,
            makespan=max(s.end for s in selected),
            tie_break=canonical_schedule_serialization(
                selected, plan.deferred),
        ),
    )


def inversion_starved_actions(
    instance: BatchInstance,
    plan: SchedulePlan,
    base: ObjectiveTuple,
    mandatory: Sequence[str],
    candidate: str,
) -> tuple[tuple[str, ...], int]:
    """Mandatory-closure actions that waited BEHIND lower-priority work.

    This is the phenomenon §7.2 names when it says a configuration must
    not starve a mandatory prerequisite, and the single-move probe cannot
    see it: under a work-conserving planner the slot a mandatory action
    wanted is not idle, it is OCCUPIED, and the question is whether the
    occupant should have waited instead.

    A mandatory-closure action is INVERSION-STARVED when there is an
    integer ``t`` in ``[ready, start)`` such that

    1. some strictly-lower-priority action — an action outside the
       mandatory closure — is running at ``t`` in the submitted plan, and
    2. the plan built by :func:`_displaced_plan` — the mandatory action at
       ``t``, the lower-priority work it displaces delayed by that
       action's duration — is ACCEPTED by the INDEPENDENT CHECKER.

    Condition 2 is what keeps this checker-consistent: "the mandatory
    action could have gone first" means exactly what capacity, hard caps,
    conflicts, groups, and prerequisites already mean, and this module
    adds no second opinion. Condition 1 is what separates an inversion
    from the single-move case, so the two metrics count different things
    and neither absorbs the other.

    Returns the inversion-starved action ids and the number of actions
    actually PROBED."""
    inverted: list[str] = []
    probed = 0
    mandatory_set = set(mandatory)
    by_id = {s.action_id: s for s in plan.selected}
    lower_priority = tuple(sorted(
        s.action_id for s in plan.selected
        if s.action_id not in mandatory_set))
    for action_id in sorted(mandatory):
        scheduled = by_id.get(action_id)
        if scheduled is None:
            continue
        ready = _ready_time(instance, plan, action_id)
        if scheduled.start <= ready:
            continue
        occupied = sorted(
            t for t in range(ready, scheduled.start)
            if any(by_id[other].start <= t < by_id[other].end
                   for other in lower_priority)
        )
        if not occupied:
            continue
        probed += 1
        for start in occupied:
            displaced = _displaced_plan(
                plan, base, action_id, start, lower_priority)
            if _accepted(instance, displaced, candidate):
                inverted.append(action_id)
                break
    return tuple(inverted), probed


def _utilization(
    instance: BatchInstance, plan: SchedulePlan
) -> Mapping[str, str]:
    """Per-pool utilisation, as an UNREDUCED 'consumed/available' string.

    Sums the DECLARED capacity curve through the trusted base's own
    accessor. That is arithmetic over a declaration, not a legality
    judgement — the checker remains the only thing that decides whether a
    capacity HELD."""
    makespan = max((s.end for s in plan.selected), default=0)
    out: dict[str, str] = {}
    for resource in sorted(instance.resources, key=lambda r: r.id):
        consumed = 0
        for scheduled in plan.selected:
            action = instance.action_by_id(scheduled.action_id)
            if action is None:
                continue
            demand = action.demand.get(resource.id)
            if isinstance(demand, Unknown) or demand is None:
                out[resource.id] = "unknown"
                break
            if isinstance(action.duration, Unknown):
                out[resource.id] = "unknown"
                break
            consumed += demand * action.duration
        else:
            available = 0
            unknown = False
            for time in range(makespan):
                capacity = resource.capacity_at(time)
                if isinstance(capacity, Unknown):
                    unknown = True
                    break
                available += capacity
            if unknown:
                out[resource.id] = "unknown"
            elif available == 0:
                out[resource.id] = "not_applicable:empty_window"
            else:
                out[resource.id] = f"{consumed}/{available}"
    return out


@dataclass(frozen=True)
class ReplayOutcome:
    """One (workload, configuration, candidate) row.

    Every judgement field comes from the checker. The binding fields at
    the top are what makes a row comparable to another row at all."""

    workload: str
    workload_digest: str
    config: str
    config_digest: str
    candidate: str
    roster_version: str
    manifest_digest: str
    status: str
    causes: tuple[str, ...]
    #: The checker said checked_feasible with no findings. Not a
    #: re-derivation and not a softening: exactly its verdict.
    qualified: bool
    objective: ObjectiveTuple | None
    makespan: int | None
    utilization: Mapping[str, str]
    queue_delay_total: int | None
    queue_delay_max: int | None
    mandatory_delay_total: int | None
    mandatory_delay_max: int | None
    single_move_starved: tuple[str, ...]
    single_move_probes: int
    inversion_starved: tuple[str, ...]
    inversion_probes: int
    capacity_violation_causes: tuple[str, ...]
    not_established: tuple[str, ...]
    lane_validity: str
    lane_excluded: tuple[str, ...]
    plan_digest: str
    certificate_digest: str

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "workload": self.workload,
            "workload_digest": self.workload_digest,
            "config": self.config,
            "config_digest": self.config_digest,
            "candidate": self.candidate,
            "roster_version": self.roster_version,
            "manifest_digest": self.manifest_digest,
            "status": self.status,
            "causes": list(self.causes),
            "qualified": self.qualified,
            "objective": (None if self.objective is None
                          else self.objective.to_canonical_dict()),
            "makespan": self.makespan,
            "utilization": dict(sorted(self.utilization.items())),
            "queue_delay_total": self.queue_delay_total,
            "queue_delay_max": self.queue_delay_max,
            "mandatory_delay_total": self.mandatory_delay_total,
            "mandatory_delay_max": self.mandatory_delay_max,
            "single_move_starved": list(self.single_move_starved),
            "single_move_probes": self.single_move_probes,
            "inversion_starved": list(self.inversion_starved),
            "inversion_probes": self.inversion_probes,
            "capacity_violation_causes": list(self.capacity_violation_causes),
            "not_established": list(self.not_established),
            "lane_validity": self.lane_validity,
            "lane_excluded": list(self.lane_excluded),
            "plan_digest": self.plan_digest,
            "certificate_digest": self.certificate_digest,
        }

    def binding(self) -> tuple[Any, ...]:
        """The comparison-binding tuple (:data:`COMPARISON_BINDING_FIELDS`)."""
        return (self.candidate, self.manifest_digest, self.roster_version,
                self.workload_digest)


def _run_one(
    workload: Workload,
    config: ResourceConfig,
    instance: BatchInstance,
    candidate: str,
    manifest_digest: str,
) -> ReplayOutcome:
    validity, excluded = lane_config_validity(workload, config)
    common: dict[str, Any] = {
        "workload": workload.name,
        "workload_digest": workload.digest,
        "config": config.name,
        "config_digest": config.digest,
        "candidate": candidate,
        "roster_version": REPLAY_ROSTER_VERSION,
        "manifest_digest": manifest_digest,
        "lane_validity": validity,
        "lane_excluded": excluded,
    }
    try:
        plan = _plan_candidate(instance, candidate)
    except BaselineRefused as refusal:
        return ReplayOutcome(
            status=STATUS_PLANNER_REFUSED,
            causes=(f"planner:{refusal.data.get('code', 'unnamed')}",),
            qualified=False, objective=None, makespan=None, utilization={},
            queue_delay_total=None, queue_delay_max=None,
            mandatory_delay_total=None, mandatory_delay_max=None,
            single_move_starved=(), single_move_probes=0,
            inversion_starved=(), inversion_probes=0,
            capacity_violation_causes=(), not_established=(),
            plan_digest="", certificate_digest="", **common)

    certificate = build_certificate(
        instance, plan, producer=_producer(candidate),
        optimality_artifact_digest=ARTIFACT_UNBOUND)
    result: ScheduleCheckResult = check_schedule(instance, plan, certificate)

    violations = tuple(sorted({
        f.cause for f in result.findings
        if f.cause in CAPACITY_VIOLATION_CAUSES
    }))
    mandatory = mandatory_closure(workload)
    delays = {
        s.action_id: s.start - _ready_time(instance, plan, s.action_id)
        for s in plan.selected
    }
    mandatory_delays = [
        delay for action_id, delay in delays.items() if action_id in mandatory
    ]
    starved: tuple[str, ...] = ()
    probes = 0
    inverted: tuple[str, ...] = ()
    inversion_probes = 0
    if result.accepted and result.recomputed_objective is not None:
        starved, probes = single_move_starved_actions(
            instance, plan, result.recomputed_objective, mandatory, candidate)
        inverted, inversion_probes = inversion_starved_actions(
            instance, plan, result.recomputed_objective, mandatory, candidate)

    return ReplayOutcome(
        status=(STATUS_CHECKED_FEASIBLE if result.accepted
                else STATUS_REFUSED),
        causes=result.causes(),
        qualified=result.accepted and not result.findings,
        objective=result.recomputed_objective,
        makespan=(None if result.recomputed_objective is None
                  else result.recomputed_objective.makespan),
        utilization=_utilization(instance, plan),
        queue_delay_total=sum(delays.values()) if delays else 0,
        queue_delay_max=max(delays.values()) if delays else 0,
        mandatory_delay_total=sum(mandatory_delays) if mandatory_delays else 0,
        mandatory_delay_max=max(mandatory_delays) if mandatory_delays else 0,
        single_move_starved=starved,
        single_move_probes=probes,
        inversion_starved=inverted,
        inversion_probes=inversion_probes,
        capacity_violation_causes=violations,
        not_established=result.not_established,
        plan_digest=plan.digest,
        certificate_digest=certificate.digest,
        **common)


# --------------------------------------------------------------------------- #
# The capacity event witness (§7: capacities apply at every breakpoint).       #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CapacityEventWitness:
    """One instance/plan pair where an INTERIOR breakpoint decides.

    The task's contract asks this lane to demonstrate that the checker's
    every-capacity-event discipline is exercised, not merely relied on.
    A start-and-end-only capacity scan would accept the plan below: the
    capacity drop that refuses it happens strictly INSIDE a running
    action's interval, at a time that is nobody's start and nobody's end.
    The witness reports the decisive time, the action whose window
    contains it, and the checker's cause."""

    instance_id: str
    breakpoint_time: int
    interior_to: tuple[str, ...]
    refused: bool
    causes: tuple[str, ...]
    #: The same plan under a FLAT capacity at the pre-drop level, which
    #: the checker accepts. Without it, "the breakpoint decided" would be
    #: indistinguishable from "the plan was infeasible anyway".
    control_accepted: bool
    control_causes: tuple[str, ...]

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "breakpoint_time": self.breakpoint_time,
            "interior_to": list(self.interior_to),
            "refused": self.refused,
            "causes": list(self.causes),
            "control_accepted": self.control_accepted,
            "control_causes": list(self.control_causes),
        }


def _capacity_witness_raw(segments: Sequence[Mapping[str, int]]
                          ) -> dict[str, Any]:
    actions = [
        _work("w_a", covers=["o_must_a"], duration=4, execution_cost=1),
        _work("w_b", action_class="repair", covers=["o_must_b"], duration=4,
              execution_cost=1),
    ]
    for action in actions:
        action["demand"] = {"pool": 1}
    return {
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "instance_id": "e4-capacity-event-witness",
        "epoch": 0,
        "cost_unit": COST_UNIT,
        "unit_mapping_version": UNIT_MAPPING_VERSION,
        "changes": [{"id": "c1", "affected_scope": ["loc:w_a", "loc:w_b"]}],
        "obligations": [
            _obligation("o_must_a", PRIORITY_MUST, "witness-a",
                        obligation_class="mandatory"),
            _obligation("o_must_b", PRIORITY_MUST, "witness-b",
                        obligation_class="mandatory"),
        ],
        "actions": actions,
        "conflict_groups": [],
        "resources": [{
            "id": "pool", "version": 1,
            "segments": [dict(s) for s in segments],
            "reservation": 10_000, "hard_cap": 99,
        }],
    }


#: The interior breakpoint: capacity 2 until t=2, then 1. Both witness
#: actions run [0, 4), so t=2 is strictly inside both windows and is
#: neither a start nor an end.
CAPACITY_WITNESS_BREAKPOINT = 2


def capacity_event_witness() -> CapacityEventWitness:
    """Build, check, and report the interior-breakpoint witness.

    Two actions of duration 4 both start at 0 and both demand one unit of
    ``pool``. Capacity is 2 until ``t=2`` and 1 from then on. Every
    action start is 0 and every action end is 4, so a scan that looked
    only at those times would see usage 2 against capacity 2 and pass.
    The checker also evaluates the breakpoint, where usage 2 meets
    capacity 1, and refuses. The control runs the identical plan against
    a FLAT capacity of 2 and is accepted, which is what makes the
    refusal attributable to the breakpoint rather than to the plan.

    ONE plan is submitted twice. The two instances differ in exactly one
    field — the capacity segments — so the plan object is a legal plan
    for both, and each certificate is rebuilt against the instance it is
    submitted with. That is what turns "the breakpoint decided" from a
    claim into a difference of one declaration."""
    drop = load_batch_instance(_capacity_witness_raw((
        {"start": 0, "capacity": 2},
        {"start": CAPACITY_WITNESS_BREAKPOINT, "capacity": 1},
    )))
    flat = load_batch_instance(_capacity_witness_raw((
        {"start": 0, "capacity": 2},
    )))
    plan = _plan_candidate(flat, "fifo")
    control = check_schedule(
        flat, plan,
        build_certificate(flat, plan, producer=_producer("fifo"),
                          optimality_artifact_digest=ARTIFACT_UNBOUND))
    dropped = check_schedule(
        drop, plan,
        build_certificate(drop, plan, producer=_producer("fifo"),
                          optimality_artifact_digest=ARTIFACT_UNBOUND))
    interior = tuple(sorted(
        s.action_id for s in plan.selected
        if s.start < CAPACITY_WITNESS_BREAKPOINT < s.end
    ))
    return CapacityEventWitness(
        instance_id=drop.instance_id,
        breakpoint_time=CAPACITY_WITNESS_BREAKPOINT,
        interior_to=interior,
        refused=not dropped.accepted,
        causes=dropped.causes(),
        control_accepted=control.accepted,
        control_causes=control.causes(),
    )


# --------------------------------------------------------------------------- #
# The preregistered capacity-matched comparison.                               #
# --------------------------------------------------------------------------- #
_STAGE_LABELS: tuple[str, ...] = (
    "stage_1_should_units",
    "stage_2_may_units",
    "stage_3_total_cost",
    "stage_4_makespan",
)


#: How many stages of the §8 ordering the capacity-matched comparison
#: uses. The ordering ITSELF is never re-derived here: the comparison
#: takes the trusted base's :func:`~...batch_schedule.objective_sort_key`
#: and TRUNCATES it. A local re-derivation of the same four stages was
#: EWD-BRR-1 — three of its four stages could be deleted without any test
#: or any report byte noticing, because this corpus never lets stages 1-3
#: decide.
#:
#: Stage 5 is excluded, and why: the tie-break is a canonical
#: serialization of the plan, and the two arms of a capacity-matched pair
#: carry different instance ids and different pool ids by construction —
#: so stage 5 would decide every single comparison on a difference that
#: has nothing to do with capacity. The exclusion is one stage wide, is
#: preregistered in ``primary_metric``, and applies ONLY to this
#: comparison: every row's reported objective still carries all five
#: stages.
COMPARISON_STAGES = 4


def _stage_key(objective: ObjectiveTuple) -> tuple[Any, ...]:
    """Stages 1-4 of the §8 ordering, TAKEN from the trusted base.

    One line, and deliberately so: the whole body is a truncation of
    :func:`~...batch_schedule.objective_sort_key`, so the ordering this
    comparison sorts by cannot drift from the ordering §8 defines."""
    return objective_sort_key(objective)[:COMPARISON_STAGES]


def _deciding_stage(base: ObjectiveTuple, rival: ObjectiveTuple) -> str:
    deltas = (
        rival.should_units - base.should_units,
        rival.may_units - base.may_units,
        base.total_cost - rival.total_cost,
        base.makespan - rival.makespan,
    )
    for index, delta in enumerate(deltas):
        if delta != 0:
            return _STAGE_LABELS[index]
    return "stage_0_identical"


@dataclass(frozen=True)
class ConfigComparisonRow:
    """ONE lane configuration against its capacity-matched shared pool."""

    config: str
    matched_shared: str
    total_capacity: int
    #: EWD-BRR-6: two of the eight matched pools are additions, not
    #: quotations, and a reader of THIS row must be able to see that
    #: without going back to the configuration list.
    config_design_named: bool
    matched_shared_design_named: bool
    better: int
    tied: int
    worse: int
    #: The denominator, spelled out beside the counts that shrank it.
    comparable: int
    not_comparable: int
    #: EWD-BRR-2: how many of the pairs above were lost to a REFUSAL —
    #: either arm's planner refusing or the checker refusing its plan.
    #: A refusal that only shows up as a smaller denominator is a
    #: shrunken experiment nobody can see.
    refused: int
    binding_mismatch: int
    #: Unreduced, so the evidence behind the fraction is visible.
    beats_matched_pool: str
    deciding_stages: Mapping[str, int]
    #: EWD-BRR-8: the SECONDARY, non-deciding paired comparison over §11
    #: E4's mandatory-prerequisite-delay metric. Lower is better.
    mandatory_delay_better: int
    mandatory_delay_tied: int
    mandatory_delay_worse: int

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "config": self.config,
            "matched_shared": self.matched_shared,
            "total_capacity": self.total_capacity,
            "config_design_named": self.config_design_named,
            "matched_shared_design_named": self.matched_shared_design_named,
            "better": self.better,
            "tied": self.tied,
            "worse": self.worse,
            "comparable": self.comparable,
            "not_comparable": self.not_comparable,
            "refused": self.refused,
            "binding_mismatch": self.binding_mismatch,
            "beats_matched_pool": self.beats_matched_pool,
            "deciding_stages": dict(sorted(self.deciding_stages.items())),
            "secondary_mandatory_delay": {
                "better": self.mandatory_delay_better,
                "tied": self.mandatory_delay_tied,
                "worse": self.mandatory_delay_worse,
                "decides": "nothing; see manifest.secondary_metrics",
            },
        }


@dataclass(frozen=True)
class ComparisonRow:
    """The preregistered capacity-matched comparison, computed not authored."""

    threshold: str
    best_config: str
    best_achieved: str
    met: bool
    per_config: tuple[ConfigComparisonRow, ...]
    #: Lane configurations whose checked outcome is identical to their
    #: matched shared pool on EVERY comparable pair, each with the REASON
    #: (EWD-BRR-4). A cap that could not bind on any workload in the
    #: corpus is a theorem of the arithmetic, not a measurement, and this
    #: row says which of the two it is.
    indistinguishable_from_shared: tuple[Mapping[str, Any], ...]
    #: The denominator the conclusion rests on, the floor it had to clear,
    #: and whether it cleared it (EWD-BRR-2).
    best_comparable: int
    minimum_comparable_pairs: int
    established: bool
    #: DERIVED from the rows above, never authored.
    headline: str
    conclusion: str

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "threshold": self.threshold,
            "best_config": self.best_config,
            "best_achieved": self.best_achieved,
            "met": self.met,
            "per_config": [r.to_canonical_dict() for r in self.per_config],
            "indistinguishable_from_shared": [
                dict(sorted(row.items()))
                for row in self.indistinguishable_from_shared],
            "best_comparable": self.best_comparable,
            "minimum_comparable_pairs": self.minimum_comparable_pairs,
            "established": self.established,
            "headline": self.headline,
            "conclusion": self.conclusion,
        }


def _light_lane_cap_bindability(
    config: ResourceConfig, corpus: Sequence[Workload]
) -> tuple[str, str] | None:
    """Can the borrowing-ON light-lane cap bind on ANY corpus workload?

    Answered from the workload/config ARITHMETIC, before any plan exists
    (EWD-BRR-4): the cap is a ``max_concurrent`` over the light members,
    so it is unbindable when no workload in the corpus even HAS more
    light actions than the light share. A row that reports such a
    configuration as "indistinguishable from its matched shared pool" is
    reporting a theorem of the corpus as if it were a measurement.
    Returns ``None`` when the cap CAN bind somewhere."""
    if config.kind != KIND_LANE or config.borrowing != BORROWING_ON_ASYMMETRIC:
        return None
    assert config.shares is not None
    light_share = config.shares[1]
    widths = {
        workload.name: sum(
            1 for lane in lane_assignment(workload, config).values()
            if lane == LANE_LIGHT
        )
        for workload in corpus
    }
    widest = max(widths.values(), default=0)
    if widest > light_share:
        return None
    return (
        "structurally_unbindable",
        f"the light-lane cap is max_concurrent={light_share} and the widest "
        f"light lane over the whole corpus is {widest} action(s) "
        f"({', '.join(f'{n}:{w}' for n, w in sorted(widths.items()))}), so "
        f"the cap can never bind and this configuration is the matched "
        f"shared pool by ARITHMETIC. Not a measured indistinguishability: "
        f"no workload here could have distinguished them"
    )


def _comparison_row(
    outcomes: Sequence[ReplayOutcome],
    threshold: Fraction,
    corpus: Sequence[Workload] = (),
    minimum_comparable: int = MINIMUM_COMPARABLE_PAIRS,
) -> ComparisonRow:
    """Compute the preregistered comparison. Nothing here is authored."""
    by_key: dict[tuple[str, str, str], ReplayOutcome] = {
        (o.config, o.workload, o.candidate): o for o in outcomes
    }
    pairs = capacity_matched_pairs()
    by_name = {c.name: c for c in configurations()}

    rows: list[ConfigComparisonRow] = []
    identical: list[Mapping[str, Any]] = []
    for config_name in sorted(pairs):
        shared_name = pairs[config_name]
        better = tied = worse = not_comparable = mismatched = refused = 0
        delay_better = delay_tied = delay_worse = 0
        stages: dict[str, int] = {}
        all_identical = True
        comparable_seen = False
        for outcome in sorted(
            (o for o in outcomes if o.config == config_name),
            key=lambda o: (o.workload, o.candidate),
        ):
            base = by_key.get((shared_name, outcome.workload, outcome.candidate))
            if base is None:
                not_comparable += 1
                all_identical = False
                continue
            if base.binding() != outcome.binding():
                # SIT-1: two arms that do not agree on every binding
                # field are two experiments, and subtracting them would
                # produce a number about nothing.
                mismatched += 1
                all_identical = False
                continue
            if (base.status != STATUS_CHECKED_FEASIBLE
                    or outcome.status != STATUS_CHECKED_FEASIBLE
                    or base.objective is None or outcome.objective is None):
                not_comparable += 1
                # EWD-BRR-2: name the reason the denominator shrank.
                if (base.status != STATUS_CHECKED_FEASIBLE
                        or outcome.status != STATUS_CHECKED_FEASIBLE):
                    refused += 1
                all_identical = False
                continue
            comparable_seen = True
            stage = _deciding_stage(base.objective, outcome.objective)
            stages[stage] = stages.get(stage, 0) + 1
            if stage != "stage_0_identical":
                all_identical = False
            lane_key = _stage_key(outcome.objective)
            base_key = _stage_key(base.objective)
            if lane_key < base_key:
                better += 1
            elif lane_key == base_key:
                tied += 1
            else:
                worse += 1
            # EWD-BRR-8: the secondary, non-deciding surface, over the
            # SAME comparable pairs. Lower delay is better.
            lane_delay = outcome.mandatory_delay_total or 0
            base_delay = base.mandatory_delay_total or 0
            if lane_delay < base_delay:
                delay_better += 1
            elif lane_delay == base_delay:
                delay_tied += 1
            else:
                delay_worse += 1
        comparable = better + tied + worse
        rows.append(ConfigComparisonRow(
            config=config_name,
            matched_shared=shared_name,
            total_capacity=by_name[config_name].total_capacity,
            config_design_named=by_name[config_name].design_named,
            matched_shared_design_named=by_name[shared_name].design_named,
            better=better, tied=tied, worse=worse,
            comparable=comparable,
            not_comparable=not_comparable, refused=refused,
            binding_mismatch=mismatched,
            beats_matched_pool=f"{better}/{comparable}",
            deciding_stages=stages,
            mandatory_delay_better=delay_better,
            mandatory_delay_tied=delay_tied,
            mandatory_delay_worse=delay_worse,
        ))
        if all_identical and comparable_seen:
            structural = _light_lane_cap_bindability(
                by_name[config_name], corpus)
            reason, detail = structural if structural else (
                "measured_indistinguishable",
                "every comparable pair produced an identical checked "
                "objective, and no configuration arithmetic forced it",
            )
            identical.append({
                "config": config_name,
                "matched_shared": shared_name,
                "reason": reason,
                "detail": detail,
            })

    best_config = ""
    best_achieved = Fraction(0, 1)
    best_text = "0/0"
    best_comparable = 0
    # ``rows`` is in configuration-name order, so a tie keeps the first
    # name deterministically rather than whichever one happened to sort
    # last in a dict.
    for row in rows:
        achieved = (Fraction(row.better, row.comparable) if row.comparable
                    else Fraction(0, 1))
        if not best_config or achieved > best_achieved:
            best_config = row.config
            best_achieved = achieved
            best_text = row.beats_matched_pool
            best_comparable = row.comparable
    # EWD-BRR-2: "measured nothing" and "measured and lost" were the same
    # value. They are not the same value.
    established = best_comparable >= max(1, minimum_comparable)
    met = established and best_achieved >= threshold and best_achieved > 0
    winners = [row.config for row in rows if row.better > 0]
    refused_total = sum(row.refused for row in rows)
    stage_totals: dict[str, int] = {}
    for row in rows:
        for stage, count in row.deciding_stages.items():
            stage_totals[stage] = stage_totals.get(stage, 0) + count
    unused = [label for label in _STAGE_LABELS if not stage_totals.get(label)]
    identical_names = [str(row["config"]) for row in identical]
    unbindable = [str(row["config"]) for row in identical
                  if row["reason"] == "structurally_unbindable"]
    delay_winners = [row.config for row in rows
                     if row.mandatory_delay_better > 0]
    if not established:
        headline = (
            f"NOT ESTABLISHED: the best comparable-pair denominator is "
            f"{best_comparable}, beneath the preregistered floor of "
            f"{minimum_comparable}. {refused_total} pair(s) were lost to a "
            f"REFUSAL. No lane conclusion is drawn from this run"
        )
    else:
        headline = (
            f"{len(winners)} of {len(rows)} lane configuration(s) ever "
            f"strictly beat their capacity-matched shared pool on the "
            f"PRIMARY metric [{', '.join(winners) or 'none'}]; best is "
            f"{best_config or 'none'} at {best_text} of comparable "
            f"(workload, candidate) pairs against a preregistered threshold "
            f"of {threshold.numerator}/{threshold.denominator} and a "
            f"preregistered minimum denominator of {minimum_comparable}; "
            f"{refused_total} pair(s) lost to a REFUSAL; "
            f"{len(identical_names)} configuration(s) are INDISTINGUISHABLE "
            f"from the matched pool on every comparable pair "
            f"[{', '.join(identical_names) or 'none'}], of which "
            f"[{', '.join(unbindable) or 'none'}] are STRUCTURALLY "
            f"unbindable rather than measured; §8 stages that decided "
            f"nothing here: [{', '.join(unused) or 'none'}]; SECONDARY and "
            f"non-deciding, on §11 E4's mandatory-prerequisite-delay "
            f"metric, {len(delay_winners)} of {len(rows)} lane "
            f"configuration(s) ever strictly beat their matched pool "
            f"[{', '.join(delay_winners) or 'none'}]"
        )
    if not established:
        conclusion = CONCLUSION_NOT_ESTABLISHED
    elif met:
        conclusion = CONCLUSION_HARDCODING_SUPPORTED
    else:
        conclusion = CONCLUSION_NOT_HARDCODED
    return ComparisonRow(
        threshold=f"{threshold.numerator}/{threshold.denominator}",
        best_config=best_config,
        best_achieved=best_text,
        met=met,
        per_config=tuple(rows),
        indistinguishable_from_shared=tuple(identical),
        best_comparable=best_comparable,
        minimum_comparable_pairs=minimum_comparable,
        established=established,
        headline=headline,
        conclusion=conclusion,
    )


# --------------------------------------------------------------------------- #
# The report.                                                                  #
# --------------------------------------------------------------------------- #
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
class E4Report:
    """The whole E4 replay. ``verdict`` is DERIVED, never authored.

    There is no timing field anywhere in this object: §11 E4's metric
    list contains no wall-time, so this lane reads no clock at all and
    there is nothing that could reach the digest."""

    manifest_digest: str
    workloads: tuple[Mapping[str, Any], ...]
    configurations: tuple[Mapping[str, Any], ...]
    outcomes: tuple[ReplayOutcome, ...]
    comparison: ComparisonRow
    capacity_event: CapacityEventWitness
    falsifications: tuple[FalsificationRow, ...]
    deferred: Mapping[str, str]

    def fired(self) -> tuple[str, ...]:
        return tuple(f.clause for f in self.falsifications if f.fired)

    def refusal_census(self) -> Mapping[str, int]:
        """Every grid row that did NOT reach the checker's feasible verdict.

        EWD-BRR-2: a refusing row shrinks every denominator downstream of
        it, and the only trace it used to leave was a smaller number.
        This census is a top-level report field, the verdict names its
        total, and the per-configuration counts sit beside the
        denominators they shrank."""
        census: dict[str, int] = {}
        for outcome in self.outcomes:
            if outcome.status == STATUS_CHECKED_FEASIBLE:
                continue
            census[f"status:{outcome.status}"] = (
                census.get(f"status:{outcome.status}", 0) + 1)
            census[f"config:{outcome.config}"] = (
                census.get(f"config:{outcome.config}", 0) + 1)
            census[f"workload:{outcome.workload}"] = (
                census.get(f"workload:{outcome.workload}", 0) + 1)
        census["total"] = sum(
            1 for o in self.outcomes if o.status != STATUS_CHECKED_FEASIBLE)
        return census

    @property
    def verdict(self) -> str:
        return self._verdict_stem() + self._refusal_suffix()

    def _refusal_suffix(self) -> str:
        """A shrunken experiment is loud IN the verdict, not inferable."""
        refused = self.refusal_census()["total"]
        if not refused:
            return ""
        return f":{VERDICT_REFUSALS_SUFFIX}:{refused}"

    def _verdict_stem(self) -> str:
        if not self.outcomes:
            return "invalid:no_outcomes"
        if not any(o.status == STATUS_CHECKED_FEASIBLE for o in self.outcomes):
            return "invalid:no_checked_feasible_candidate"
        if not self.capacity_event.refused:
            return "invalid:capacity_event_witness_did_not_fire"
        for row in self.falsifications:
            if row.fired and row.zero_tolerance:
                return f"refuted:{row.clause}"
        # EWD-BRR-2: a comparison over too few comparable pairs may not
        # reject the lane claim EITHER. "We measured nothing" is not a
        # negative result, and it is not a clear run.
        if not self.comparison.established:
            return "invalid:comparison_denominator_below_minimum"
        for row in self.falsifications:
            if row.fired:
                return f"lane_claim_rejected:{row.clause}"
        # Nothing fired. That is a clear run of WHAT RAN, and what ran is
        # not all of E4: symmetric borrowing, the history-window data
        # clause, the word "realized" in E4's own metric list, and the
        # unit-demand bound on the lane compilation are deferred. A bare
        # "pass" would read as E4 passed, so the partiality is carried IN
        # the verdict, derived from the deferral rows.
        partial = tuple(
            name for name in PARTIAL_E4_DEFERRALS if name in self.deferred)
        if not partial:
            return VERDICT_PASS
        return f"{VERDICT_PASS_PARTIAL_E4}:{','.join(partial)}"

    def lanes_conclusion(self) -> str:
        """The lane conclusion, or a refusal to publish one.

        A substantive claim about lanes beneath an ``invalid:`` verdict is
        a claim the run did not earn, whatever the denominator was."""
        if self._verdict_stem().startswith("invalid:"):
            if not self.comparison.established:
                return CONCLUSION_NOT_ESTABLISHED
            return CONCLUSION_NOT_ESTABLISHED_INVALID
        return self.comparison.conclusion

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "schema": "okg.synth.e4-resource-replay-report.v1",
            "manifest_digest": self.manifest_digest,
            "verdict": self.verdict,
            "lanes_conclusion": self.lanes_conclusion(),
            "refusal_census": dict(sorted(self.refusal_census().items())),
            "checker_version": CHECKER_VERSION,
            "producer_version": PRODUCER_VERSION,
            "workloads": [dict(sorted(w.items())) for w in self.workloads],
            "configurations": [dict(sorted(c.items()))
                               for c in self.configurations],
            "outcomes": [o.to_canonical_dict() for o in self.outcomes],
            "comparison": self.comparison.to_canonical_dict(),
            "capacity_event_witness": self.capacity_event.to_canonical_dict(),
            "falsifications": [
                f.to_canonical_dict() for f in self.falsifications],
            "deferred": dict(sorted(self.deferred.items())),
        }

    def canonical_json(self) -> str:
        return json.dumps(
            self.to_canonical_dict(), sort_keys=True, separators=(",", ":")
        )

    def digest(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


def run_report(manifest: E4Manifest) -> E4Report:
    """Run the whole preregistered E4 grid.

    For each workload: compile it under every configuration, plan it with
    every candidate, and hand every plan to the independent checker."""
    corpus = workloads(manifest)
    grid = configurations()
    manifest_digest = manifest.digest()

    workload_rows: list[Mapping[str, Any]] = []
    outcomes: list[ReplayOutcome] = []
    for workload in corpus:
        workload_rows.append({
            "name": workload.name,
            "source": workload.source,
            "exercises": workload.exercises,
            "workload_digest": workload.digest,
            "actions": len(workload.actions),
            "obligations": len(workload.obligations),
            "mandatory_closure": ",".join(mandatory_closure(workload)),
        })
        for config in grid:
            instance = load_batch_instance(compile_instance(workload, config))
            for candidate, _ in CANDIDATES:
                outcomes.append(_run_one(
                    workload, config, instance, candidate, manifest_digest))

    threshold_text = manifest.comparator_threshold.split("/")
    threshold = Fraction(int(threshold_text[0]), int(threshold_text[1]))
    comparison = _comparison_row(
        outcomes, threshold, corpus, manifest.minimum_comparable_pairs)
    witness = capacity_event_witness()

    lane_rows = [o for o in outcomes if o.lane_validity != LANE_CONFIG_VALID]
    starved = [
        f"{o.workload}/{o.config}/{o.candidate}:{action}"
        for o in outcomes for action in o.single_move_starved
    ]
    # EWD-BRR-3, scoped from §7.2's own conditional: the lane promise
    # ("IF a critical lane is configured, it must admit the full
    # prerequisite closure of mandatory actions") binds only a
    # configuration that CONFIGURES a lane. Inversions under a shared or
    # heterogeneous pool are measured and reported; they are not a
    # configuration invalidity, because that configuration promised
    # nothing about lanes.
    lane_configs = {c.name for c in grid if c.kind == KIND_LANE}
    inverted_lane = [
        f"{o.workload}/{o.config}/{o.candidate}:{action}"
        for o in outcomes if o.config in lane_configs
        for action in o.inversion_starved
    ]
    inverted_pool = [
        f"{o.workload}/{o.config}/{o.candidate}:{action}"
        for o in outcomes if o.config not in lane_configs
        for action in o.inversion_starved
    ]
    violating = [
        f"{o.workload}/{o.config}/{o.candidate}:{cause}"
        for o in outcomes for cause in o.capacity_violation_causes
    ]
    read_as_pass = [
        f"{o.workload}/{o.config}/{o.candidate}"
        for o in outcomes
        if o.qualified and (o.status != STATUS_CHECKED_FEASIBLE or o.causes)
    ]

    falsifications = (
        FalsificationRow(
            clause=CLAUSE_CHECKER_REFUSAL_READ_AS_PASS,
            fired=bool(read_as_pass),
            zero_tolerance=True,
            evaluated=len(outcomes),
            detail=("every reported qualification is the checker's own "
                    "checked_feasible verdict with no findings"
                    if not read_as_pass
                    else "REFUSAL REPORTED AS QUALIFIED: "
                         + ",".join(sorted(read_as_pass))),
        ),
        FalsificationRow(
            clause=CLAUSE_LANE_EXCLUDES_MANDATORY_CLOSURE,
            fired=bool(lane_rows),
            zero_tolerance=True,
            evaluated=sum(
                1 for o in outcomes
                if any(c.name == o.config and c.lane_rule != LANE_RULE_NONE
                       for c in grid)),
            detail=("every configured critical lane admits the full "
                    "prerequisite closure of the mandatory actions (§7.2)"
                    if not lane_rows
                    else "LANE CONFIGURATION EXCLUDES MANDATORY CLOSURE "
                         "MEMBERS: " + ",".join(sorted({
                             f"{o.workload}/{o.config}:"
                             f"{','.join(o.lane_excluded)}"
                             for o in lane_rows}))),
        ),
        FalsificationRow(
            clause=CLAUSE_MANDATORY_PREREQUISITE_STARVED,
            fired=bool(starved),
            zero_tolerance=True,
            evaluated=sum(o.single_move_probes for o in outcomes),
            detail=(
                "no mandatory-closure action waited at a time the checker "
                "would have accepted it running ALONE; this probe never "
                "displaces lower-priority work (deferred: "
                "starvation_probe_is_single_move_only)"
                if not starved
                else "MANDATORY PREREQUISITE SINGLE-MOVE STARVED: "
                     + ",".join(sorted(starved))),
        ),
        FalsificationRow(
            clause=CLAUSE_MANDATORY_PREREQUISITE_INVERSION,
            # SCOPED, not silent: only a configuration that configures a
            # critical lane can violate §7.2's conditional promise.
            fired=bool(inverted_lane),
            zero_tolerance=True,
            evaluated=sum(o.inversion_probes for o in outcomes),
            detail=(
                (
                    "no mandatory-closure action under a LANE configuration "
                    "waited behind strictly-lower-priority work the checker "
                    "would have accepted waiting instead"
                    if not inverted_lane
                    else "MANDATORY PREREQUISITE INVERSION-STARVED UNDER A "
                         "LANE CONFIGURATION: " + ",".join(sorted(
                             inverted_lane))
                )
                + ". REPORTED, NOT VALIDITY-BEARING (§7.2 conditions the "
                "promise on a critical lane being configured, and a generic "
                f"pool configures none): {len(inverted_pool)} inversion(s) "
                "under shared or heterogeneous pools"
                + (": " + ",".join(sorted(inverted_pool))
                   if inverted_pool else "")
            ),
        ),
        FalsificationRow(
            clause=CLAUSE_REALIZED_CAPACITY_VIOLATION,
            fired=bool(violating),
            zero_tolerance=True,
            evaluated=len(outcomes),
            detail=("no submitted plan carried a capacity, hard-cap, or "
                    "reservation violation cause"
                    if not violating
                    else "NAMED CAPACITY VIOLATED: "
                         + ",".join(sorted(violating))),
        ),
        FalsificationRow(
            clause=CLAUSE_CAPACITY_MATCHED_COMPARISON,
            # A clause that could not be evaluated did not FAIL. Below
            # the preregistered minimum denominator this reports NOT
            # EVALUATED and the verdict says invalid, rather than
            # rejecting the lane claim over a measurement nobody made.
            fired=comparison.established and not comparison.met,
            zero_tolerance=False,
            evaluated=sum(row.comparable for row in comparison.per_config),
            detail=(
                (
                    f"best lane configuration beats its capacity-matched "
                    f"shared pool on {comparison.best_achieved} of "
                    f"comparable (workload, candidate) pairs against a "
                    f"preregistered threshold of {comparison.threshold} "
                    f"over a denominator of {comparison.best_comparable} "
                    f"against a preregistered minimum of "
                    f"{comparison.minimum_comparable_pairs}"
                    if comparison.established
                    else f"NOT EVALUATED: the best comparable-pair "
                         f"denominator is {comparison.best_comparable}, "
                         f"beneath the preregistered minimum of "
                         f"{comparison.minimum_comparable_pairs}"
                )
                + f". {comparison.headline} CONCLUSION: "
                  f"{comparison.conclusion}"),
        ),
    )

    return E4Report(
        manifest_digest=manifest_digest,
        workloads=tuple(workload_rows),
        configurations=tuple(c.to_canonical_dict() for c in grid),
        outcomes=tuple(outcomes),
        comparison=comparison,
        capacity_event=witness,
        falsifications=falsifications,
        deferred=dict(manifest.deferred),
    )
