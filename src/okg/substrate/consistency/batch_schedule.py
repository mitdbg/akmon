"""The batch-scheduling contract: schema, canonicalization, digests, loader.

PACT ``concord-disclosure-budget``, task.batch.contract-checker
(req.batch.checker-implementation), step 1 of the approved bounded
implementation sequence in ``designs/batch-scheduling-research.md`` §13.
Normative sections: §6 (priority and coverage units), §7 (+§7.1 cost
semantics, §7.2 resources), §8 (lexicographic objective), §12
(reproducibility binding). Program design ``concord-invariant-engine``
v8 §6 is the parent contract for the plan pecking order and MayChange.

What this module IS
-------------------
The **declared trusted base**. It is the only code an independent
checker may share with any future planner, so it holds exactly two
things and nothing else:

1. the typed, frozen schema for a batch instance, a plan, and a
   certificate; and
2. the canonicalization those objects are digested through — sorted
   keys, UTF-8, **integers and strings only**. Floats, nulls, and
   booleans REFUSE. Durations, costs, capacities, demands, and times
   are integers in the instance's declared base units; there is no
   float anywhere in the contract, so a digest is byte-stable across
   processes and platforms.

It contains NO search, NO placement heuristic, NO objective
optimization, and no notion of "best". Feasibility and value are
established by :mod:`.batch_schedule_checker`; the word *optimal* is
only expressible through an oracle artifact (§9.1).

What this module does NOT establish
-----------------------------------
Parsing an instance says nothing about whether its declared
dependencies are complete, whether its effect summaries are sound,
whether its estimates are accurate, or whether its checks are correct.
§5.1 of the design lists those as explicit non-guarantees. This module
only makes the author's claims typed, closed, and digestible.

Four fail-closed rules carry most of the safety
-----------------------------------------------
* **Unknown is a type, not a zero.** :class:`Unknown` is a distinct
  frozen type — modelled on
  :class:`...possible_effects.UnstatedChange` — used for duration,
  cost, demand, capacity, reservations, effects, and revelations. It
  can never compare equal to an integer or a location set, so no
  ``or``-style default, ``sum``, or set union can silently promote an
  absence into a permissive zero. An unknown effect widens disclosure
  to the whole affected scope (§7); an unknown revelation refuses
  qualified publication (§9); an unknown bound cannot support a
  hard-budget guarantee.
* **Priority is a closed vocabulary attached to a coverage unit.**
  ``must`` / ``should`` / ``may``. An unmapped or unrecognized
  priority REFUSES with :data:`REFUSAL_UNKNOWN_PRIORITY`; the loader
  never guesses a mandatory policy (§6).
* **Coverage counts stable versioned units, never rows.** A
  :class:`CoverageUnit` is identified by
  ``(spec_version, subject_scope_key, satisfaction_condition)``. Two
  obligations that duplicate or split the same finding map to the same
  unit and are counted ONCE, in feasibility and in ``should`` / ``may``
  credit alike. The obligation-to-unit mapping carries its own digest
  (:attr:`BatchInstance.unit_mapping_digest`) and is an input to the
  plan, so a clone/split mutation is visible as a digest change rather
  than as free objective credit.
* **Silence is not an answer.** Every action answers for EVERY declared
  resource — an omitted ``demand`` key refuses rather than reading as
  zero — and no action may declare ``duration: 0``, because an interval
  of no length satisfies no ``start <= t < end`` and would exempt the
  action from every capacity, conflict, and reservation check while
  still earning coverage credit. Zero demand and an unmeasured duration
  both stay expressible; they just have to be written down.

Deliberate duplication
----------------------
:func:`canonical_json` repeats the discipline already written in
:mod:`.invariant_spec`. That is intentional: the trusted base must stay
small and dependency-light (stdlib + :class:`SubstrateError`) so that
"the checker shares only schema and canonicalization with the planner"
is an import-graph fact, not a comment.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from okg.substrate.errors import SubstrateError

__all__ = [
    "ARTIFACT_UNBOUND",
    "CONTRACT_SCHEMA_VERSION",
    "OPTIMALITY_METHODS",
    "OPTIMALITY_METHOD_CHECKED_BOUND",
    "OPTIMALITY_METHOD_EXHAUSTIVE",
    "OUTCOMES",
    "OUTCOME_FAILURE",
    "OUTCOME_SUCCESS",
    "OUTCOME_UNKNOWN",
    "PRIORITIES",
    "PRIORITY_MAY",
    "PRIORITY_MUST",
    "PRIORITY_SHOULD",
    "REFUSAL_CODES",
    "REFUSAL_CONFLICTING_UNIT_PRIORITY",
    "REFUSAL_DANGLING_REFERENCE",
    "REFUSAL_DUPLICATE_IDENTITY",
    "REFUSAL_EMPTY_DECLARATION",
    "REFUSAL_MISSING_FIELD",
    "REFUSAL_MIXED_COST_UNITS",
    "REFUSAL_NON_CANONICAL_VALUE",
    "REFUSAL_SCHEMA_VERSION",
    "REFUSAL_UNKNOWN_FIELD",
    "REFUSAL_UNKNOWN_PRIORITY",
    "TERMINATION_EXHAUSTED",
    "TERMINATION_STATUSES",
    "TERMINATION_STOPPED",
    "UNKNOWN",
    "Action",
    "BatchInstance",
    "BatchLoadRefused",
    "CapacitySegment",
    "Change",
    "ConflictGroup",
    "CoverageUnit",
    "CoverageWitness",
    "EstimatorRef",
    "Effects",
    "MaybeInt",
    "Obligation",
    "OptimalityArtifact",
    "ObjectiveTuple",
    "ProducerRef",
    "Resource",
    "Revelation",
    "ScheduleCertificate",
    "ScheduledAction",
    "SchedulePlan",
    "Unknown",
    "canonical_json",
    "canonical_schedule_serialization",
    "enumeration_breakpoints",
    "enumeration_horizon",
    "instance_digest",
    "load_batch_instance",
    "load_certificate",
    "load_plan",
    "objective_sort_key",
    "object_digest",
    "plan_digest",
    "search_space_digest",
]


#: The contract version every instance, plan, and certificate binds. A
#: schema change is a digest change everywhere it is referenced (§12).
CONTRACT_SCHEMA_VERSION = "batch-schedule/1"


# --------------------------------------------------------------------------- #
# Refusal codes — named, closed. The loader never defaults silently.          #
# --------------------------------------------------------------------------- #
REFUSAL_UNKNOWN_FIELD = "unknown_field"
REFUSAL_MISSING_FIELD = "missing_field"
REFUSAL_NON_CANONICAL_VALUE = "non_canonical_value"
REFUSAL_UNKNOWN_PRIORITY = "unknown_priority"
REFUSAL_CONFLICTING_UNIT_PRIORITY = "conflicting_unit_priority"
REFUSAL_MIXED_COST_UNITS = "mixed_cost_units"
REFUSAL_DANGLING_REFERENCE = "dangling_reference"
REFUSAL_DUPLICATE_IDENTITY = "duplicate_identity"
REFUSAL_EMPTY_DECLARATION = "empty_declaration"
REFUSAL_SCHEMA_VERSION = "schema_version_unsupported"

REFUSAL_CODES = (
    REFUSAL_CONFLICTING_UNIT_PRIORITY,
    REFUSAL_DANGLING_REFERENCE,
    REFUSAL_DUPLICATE_IDENTITY,
    REFUSAL_EMPTY_DECLARATION,
    REFUSAL_MISSING_FIELD,
    REFUSAL_MIXED_COST_UNITS,
    REFUSAL_NON_CANONICAL_VALUE,
    REFUSAL_SCHEMA_VERSION,
    REFUSAL_UNKNOWN_FIELD,
    REFUSAL_UNKNOWN_PRIORITY,
)

#: The closed priority vocabulary of design §6. There is no fourth value
#: and no default: an unmapped policy refuses.
PRIORITY_MUST = "must"
PRIORITY_SHOULD = "should"
PRIORITY_MAY = "may"
PRIORITIES = (PRIORITY_MUST, PRIORITY_SHOULD, PRIORITY_MAY)

#: The closed witness-outcome vocabulary. Only ``success`` discharges a
#: coverage unit; ``unknown`` is an admission, never a pass.
OUTCOME_SUCCESS = "success"
OUTCOME_FAILURE = "failure"
OUTCOME_UNKNOWN = "unknown"
OUTCOMES = (OUTCOME_SUCCESS, OUTCOME_FAILURE, OUTCOME_UNKNOWN)

#: The closed set of artifacts that may license the word "optimal" (§9.1).
OPTIMALITY_METHOD_EXHAUSTIVE = "exhaustive_enumeration"
OPTIMALITY_METHOD_CHECKED_BOUND = "checked_bound"
OPTIMALITY_METHODS = (
    OPTIMALITY_METHOD_CHECKED_BOUND, OPTIMALITY_METHOD_EXHAUSTIVE,
)

#: The closed producer-termination vocabulary bound by every certificate
#: (§12, "solver family, version, parameters, random seed, and
#: termination status"). ``stopped`` is the honest default: a producer
#: that did not finish its space may not support an exhaustive claim.
TERMINATION_EXHAUSTED = "exhausted"
TERMINATION_STOPPED = "stopped"
TERMINATION_STATUSES = (TERMINATION_EXHAUSTED, TERMINATION_STOPPED)

#: The sentinel a certificate carries when it binds NO optimality
#: artifact. Spelled explicitly, never as an empty string or a null, so
#: "no artifact" and "artifact I forgot to write down" are the same
#: refusable thing: a certificate that claims optimality must name the
#: artifact's digest (§12).
ARTIFACT_UNBOUND = "unbound"


class BatchLoadRefused(SubstrateError):
    """A batch instance, plan, or certificate failed the fail-closed load.

    ``data['code']`` is one of :data:`REFUSAL_CODES`; ``data['where']``
    names the offending object path."""

    code = "batch_load_refused"


def _refuse(code: str, where: str, message: str, **extra: Any) -> None:
    raise BatchLoadRefused(
        f"batch schedule load refused [{code}] at {where}: {message}",
        data={"code": code, "where": where, **extra},
    )


# --------------------------------------------------------------------------- #
# Canonical JSON — integers and strings only; floats, nulls, bools refuse.    #
# --------------------------------------------------------------------------- #
def _check_canonical_value(value: Any, where: str) -> None:
    if value is None:
        _refuse(
            REFUSAL_NON_CANONICAL_VALUE, where,
            "null is not a value; absent means absent, and an unknown "
            "quantity is spelled as the UNKNOWN sentinel, not as null",
        )
    if isinstance(value, bool):
        _refuse(
            REFUSAL_NON_CANONICAL_VALUE, where,
            "booleans are not admitted; the canonical closure is "
            "integers and strings",
        )
    if isinstance(value, float):
        _refuse(
            REFUSAL_NON_CANONICAL_VALUE, where,
            "floats are not admitted; durations, costs, demands, and "
            "capacities are integers in the declared base unit",
        )
    if isinstance(value, Mapping):
        for k, v in value.items():
            if not isinstance(k, str):
                _refuse(REFUSAL_NON_CANONICAL_VALUE, where,
                        "mapping keys must be strings")
            _check_canonical_value(v, f"{where}.{k}")
    elif isinstance(value, (list, tuple)):
        for i, v in enumerate(value):
            _check_canonical_value(v, f"{where}[{i}]")
    elif not isinstance(value, (str, int)):
        _refuse(REFUSAL_NON_CANONICAL_VALUE, where,
                f"value of type {type(value).__name__} is not canonical")


def canonical_json(value: Any, *, where: str = "$") -> str:
    """The single canonical rendering every digest in this contract rests on."""
    _check_canonical_value(value, where)
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)


def object_digest(value: Any, *, where: str = "$") -> str:
    """sha256 over :func:`canonical_json`."""
    return hashlib.sha256(
        canonical_json(value, where=where).encode("utf-8")
    ).hexdigest()


# --------------------------------------------------------------------------- #
# UNKNOWN — a type, never a zero.                                             #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Unknown:
    """The typed ABSENCE of a quantity or a summary (design §7).

    A distinct type, never an ``int`` and never an empty set, because the
    two readings of a missing number are opposite: ``0`` is permissive
    (costs nothing, changes nothing, needs no capacity) and "not stated"
    is restrictive (no cost check, no capacity check, and no disclosure
    narrowing can be established). Conflating them would let an
    unmeasured action look free and an unsummarized action look inert.

    Because it is a different type, ``sum``, ``max``, ``|``, and
    ``value or 0`` all fail loudly rather than defaulting. Every consumer
    must name what it cannot establish."""

    reason: str = "no value was declared"

    def to_canonical_dict(self) -> dict[str, Any]:
        return {"unknown": self.reason}


#: The one canonical "not stated" value, so refusal details read alike.
UNKNOWN = Unknown()

#: A quantity that may be absent. Spelled ``{"unknown": "<reason>"}`` on
#: the wire, never ``null`` and never ``0``.
MaybeInt = int | Unknown


@dataclass(frozen=True)
class Effects:
    """A conservative set of result locations an action may change (§7).

    ``Effects`` is the STATED summary. The unstated case is
    :class:`Unknown`, and it widens disclosure to the whole affected
    scope of the instance's changes — see
    :func:`...batch_schedule_checker.derive_may_change`."""

    locations: tuple[str, ...] = ()

    def to_canonical_dict(self) -> dict[str, Any]:
        return {"locations": list(self.locations)}


@dataclass(frozen=True)
class Revelation:
    """The classes of obligations and actions an execution may ADD (§7).

    ``Revelation`` is the STATED bound. The unstated case is
    :class:`Unknown`, which §9 treats as capable of revealing every
    applicable configured ``must`` — qualified publication is refused,
    strictly, because a hidden mandatory obligation cannot be repaired
    by a warning."""

    obligation_classes: tuple[str, ...] = ()
    action_classes: tuple[str, ...] = ()

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "obligation_classes": list(self.obligation_classes),
            "action_classes": list(self.action_classes),
        }


# --------------------------------------------------------------------------- #
# Coverage units — stable versioned equivalence classes (§6).                 #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CoverageUnit:
    """The objective's unit of account.

    Identity is ``(spec_version, subject_scope_key,
    satisfaction_condition)`` — a versioned equivalence class over
    obligation instances, deliberately independent of how many physical
    finding rows an extractor emitted. Splitting one obligation into
    three, or cloning it, yields the SAME unit and therefore the same
    single credit (§6, and the E3 clone/split falsification)."""

    spec_version: str
    subject_scope_key: str
    satisfaction_condition: str

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "spec_version": self.spec_version,
            "subject_scope_key": self.subject_scope_key,
            "satisfaction_condition": self.satisfaction_condition,
        }

    @property
    def key(self) -> str:
        """The canonical unit key: unambiguous, sortable, and stable."""
        return canonical_json(self.to_canonical_dict())


@dataclass(frozen=True)
class Obligation:
    """A violation or inspection obligation mapped to exactly one unit."""

    id: str
    obligation_class: str
    priority: str
    unit: CoverageUnit

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "obligation_class": self.obligation_class,
            "priority": self.priority,
            "unit": self.unit.to_canonical_dict(),
        }


@dataclass(frozen=True)
class Change:
    """A change with stable identity and its affected scope (§7, ``C``)."""

    id: str
    affected_scope: tuple[str, ...]

    def to_canonical_dict(self) -> dict[str, Any]:
        return {"id": self.id, "affected_scope": list(self.affected_scope)}


@dataclass(frozen=True)
class EstimatorRef:
    """Estimator identity and provenance for duration/cost (§7, §12)."""

    id: str
    version: int

    def to_canonical_dict(self) -> dict[str, Any]:
        return {"id": self.id, "version": self.version}


@dataclass(frozen=True)
class Action:
    """An executable check, shared query, repair, validation, or gate.

    Actions are non-preemptive (§7): an instance that wants preemption
    must decompose the action into explicit chunks with their own
    identities, dependencies, effects, and demands. ``duration``,
    ``execution_cost``, and each ``demand`` entry may be
    :class:`Unknown`; the checker then names what it cannot establish
    instead of substituting zero."""

    id: str
    action_class: str
    #: obligation ids this action covers; one action may cover many.
    covers: tuple[str, ...]
    #: prerequisite action ids: this action may start only after each
    #: prerequisite SUCCEEDS.
    requires: tuple[str, ...]
    #: pairwise conflicts. Bounded multi-action exclusions use
    #: :class:`ConflictGroup` instead.
    conflicts: tuple[str, ...]
    reveals: Revelation | Unknown
    effects: Effects | Unknown
    duration: MaybeInt
    execution_cost: MaybeInt
    #: The action's cost unit. It MUST equal the instance's single
    #: declared unit; adding unlike units refuses (§7.1).
    cost_unit: str
    demand: Mapping[str, MaybeInt]
    estimator: EstimatorRef

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "action_class": self.action_class,
            "covers": list(self.covers),
            "requires": list(self.requires),
            "conflicts": list(self.conflicts),
            "reveals": self.reveals.to_canonical_dict(),
            "effects": self.effects.to_canonical_dict(),
            "duration": _quantity_to_canonical(self.duration),
            "execution_cost": _quantity_to_canonical(self.execution_cost),
            "cost_unit": self.cost_unit,
            "demand": {
                r: _quantity_to_canonical(v)
                for r, v in sorted(self.demand.items())
            },
            "estimator": self.estimator.to_canonical_dict(),
        }


@dataclass(frozen=True)
class ConflictGroup:
    """A named constraint node over an action set (§7).

    ``max_concurrent`` is the bounded multi-action exclusion: at most
    that many members may overlap at any schedule event. Pairwise
    ``conflicts(a, b)`` is the ``max_concurrent = 1``, two-member case
    and is expressible directly on the action for convenience."""

    id: str
    members: tuple[str, ...]
    max_concurrent: int

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "members": list(self.members),
            "max_concurrent": self.max_concurrent,
        }


@dataclass(frozen=True)
class CapacitySegment:
    """One piece of a piecewise-constant capacity configuration (§7).

    ``start`` is a schedule event: the checker evaluates capacity at
    every action start, every action end, AND every breakpoint, so a
    capacity that drops in the middle of a running action is caught."""

    start: int
    capacity: MaybeInt

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "start": self.start,
            "capacity": _quantity_to_canonical(self.capacity),
        }


@dataclass(frozen=True)
class Resource:
    """A named pool: model concurrency, CPU workers, connections, ....

    ``critical`` and ``light`` lanes are NOT modelled here. Design §7.2
    is explicit that lanes are optional configuration aliases or
    eligibility rules over these pools and not schema-level action
    kinds, and E4 decides whether they deserve a runtime surface at all.

    ``segments`` is the instantaneous concurrent capacity over time.
    ``reservation`` is the CONSUMABLE budget: the sum of
    ``demand(a, r) * duration(a)`` over selected actions. ``hard_cap``
    is an instantaneous admission cap enforced alongside the capacity
    curve. Either bound may be :class:`Unknown`, and an unknown bound
    cannot support a hard-budget guarantee (§7) — the checker records
    that guarantee as NOT ESTABLISHED rather than passing it."""

    id: str
    version: int
    segments: tuple[CapacitySegment, ...]
    reservation: MaybeInt
    hard_cap: MaybeInt

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "version": self.version,
            "segments": [s.to_canonical_dict() for s in self.segments],
            "reservation": _quantity_to_canonical(self.reservation),
            "hard_cap": _quantity_to_canonical(self.hard_cap),
        }

    def capacity_at(self, time: int) -> MaybeInt:
        """The piecewise-constant capacity in force at ``time``."""
        current: MaybeInt = UNKNOWN
        for segment in self.segments:
            if segment.start <= time:
                current = segment.capacity
            else:
                break
        return current


def _quantity_to_canonical(value: Any) -> Any:
    if isinstance(value, Unknown):
        return value.to_canonical_dict()
    return value


# --------------------------------------------------------------------------- #
# The instance.                                                                #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class BatchInstance:
    """One frozen planning epoch (§7).

    Every collection is stored in the author's declared order, which the
    baselines read as discovery order; every DIGEST is taken over a
    canonically sorted rendering, so digest identity never depends on
    declaration order while FIFO order remains reproducible."""

    schema_version: str
    instance_id: str
    epoch: int
    #: The instance's single declared scalar cost unit (§7.1).
    cost_unit: str
    #: The version of the obligation-to-coverage-unit mapping (§6).
    unit_mapping_version: str
    changes: tuple[Change, ...]
    obligations: tuple[Obligation, ...]
    actions: tuple[Action, ...]
    conflict_groups: tuple[ConflictGroup, ...]
    resources: tuple[Resource, ...]

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "instance_id": self.instance_id,
            "epoch": self.epoch,
            "cost_unit": self.cost_unit,
            "unit_mapping_version": self.unit_mapping_version,
            "changes": [c.to_canonical_dict()
                        for c in sorted(self.changes, key=lambda c: c.id)],
            "obligations": [o.to_canonical_dict() for o in
                            sorted(self.obligations, key=lambda o: o.id)],
            "actions": [a.to_canonical_dict()
                        for a in sorted(self.actions, key=lambda a: a.id)],
            "conflict_groups": [g.to_canonical_dict() for g in
                                sorted(self.conflict_groups,
                                       key=lambda g: g.id)],
            "resources": [r.to_canonical_dict()
                          for r in sorted(self.resources, key=lambda r: r.id)],
        }

    @property
    def digest(self) -> str:
        return object_digest(self.to_canonical_dict())

    def unit_mapping_canonical_dict(self) -> dict[str, Any]:
        """The obligation-to-unit mapping as a first-class plan input (§6)."""
        return {
            "unit_mapping_version": self.unit_mapping_version,
            "entries": [
                {
                    "obligation_id": o.id,
                    "priority": o.priority,
                    "unit": o.unit.to_canonical_dict(),
                }
                for o in sorted(self.obligations, key=lambda o: o.id)
            ],
        }

    @property
    def unit_mapping_digest(self) -> str:
        return object_digest(self.unit_mapping_canonical_dict())

    # -- convenience views, all deterministic ------------------------------ #
    def action_by_id(self, action_id: str) -> Action | None:
        for action in self.actions:
            if action.id == action_id:
                return action
        return None

    def obligation_by_id(self, obligation_id: str) -> Obligation | None:
        for obligation in self.obligations:
            if obligation.id == obligation_id:
                return obligation
        return None

    def resource_by_id(self, resource_id: str) -> Resource | None:
        for resource in self.resources:
            if resource.id == resource_id:
                return resource
        return None

    def units_by_priority(self, priority: str) -> tuple[str, ...]:
        """Distinct unit keys at ``priority``, canonically ordered.

        Distinct is the point: duplicate and split obligations collapse
        here, before any counting happens."""
        keys = {o.unit.key for o in self.obligations if o.priority == priority}
        return tuple(sorted(keys))

    def whole_affected_scope(self) -> tuple[str, ...]:
        """The union of every change's affected scope.

        This is what an UNKNOWN effect widens disclosure to (§7)."""
        scope: set[str] = set()
        for change in self.changes:
            scope.update(change.affected_scope)
        return tuple(sorted(scope))


def instance_digest(instance: BatchInstance) -> str:
    return instance.digest


# --------------------------------------------------------------------------- #
# The plan and its objective.                                                  #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ScheduledAction:
    """A selected action with its interval, assignment, and OUTCOME.

    ``outcome`` is drawn from :data:`OUTCOMES` and is what makes §7's
    ``requires(a, b)`` — "``a`` may start only after prerequisite ``b``
    **succeeds**" — expressible. Without it the schema could state that a
    prerequisite RAN but never that it SUCCEEDED, so the third conjunct
    of the model's central edge had nowhere to live and an action could
    collect coverage credit downstream of a failure. It has no default:
    an unreported outcome is spelled ``unknown``, which discharges
    nothing and satisfies no prerequisite."""

    action_id: str
    start: int
    end: int
    resource_assignments: Mapping[str, int]
    outcome: str

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "action_id": self.action_id,
            "start": self.start,
            "end": self.end,
            "resource_assignments": dict(
                sorted(self.resource_assignments.items())
            ),
            "outcome": self.outcome,
        }

    def to_schedule_dict(self) -> dict[str, Any]:
        """The SCHEDULE-shaped rendering used by the §8 stage-5 tie-break.

        Deliberately omits ``outcome``: stage 5 breaks ties between
        SCHEDULES, and an execution result is not a scheduling decision.
        Including it would let a plan improve its own tie-break by
        declaring a failure, which is the opposite of an incentive the
        contract should offer."""
        return {
            "action_id": self.action_id,
            "start": self.start,
            "end": self.end,
            "resource_assignments": dict(
                sorted(self.resource_assignments.items())
            ),
        }


@dataclass(frozen=True)
class CoverageWitness:
    """Evidence that one obligation's unit was discharged by one action.

    A witness with outcome other than ``success`` discharges nothing —
    an ``unknown`` outcome is an admission, not a pass."""

    unit_key: str
    obligation_id: str
    action_id: str
    outcome: str

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "unit_key": self.unit_key,
            "obligation_id": self.obligation_id,
            "action_id": self.action_id,
            "outcome": self.outcome,
        }


@dataclass(frozen=True)
class ObjectiveTuple:
    """The §8 lexicographic tuple, in comparison order.

    Stage 1 and 2 are MAXIMIZED; stages 3 and 4 are minimized; stage 5
    is the deterministic tie-break — the lexicographically smallest
    canonical schedule serialization. See :func:`objective_sort_key`.

    Note what is NOT in this tuple: mandatory coverage. Every known
    ``must`` unit covered, and no deferred effect able to reveal a
    ``must``, is a HARD FEASIBILITY CONDITION (§8), equivalent to zero
    unresolved ``must`` — never a penalty term that a large enough
    ``should`` count could outweigh."""

    should_units: int
    may_units: int
    total_cost: int
    makespan: int
    tie_break: str

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "should_units": self.should_units,
            "may_units": self.may_units,
            "total_cost": self.total_cost,
            "makespan": self.makespan,
            "tie_break": self.tie_break,
        }


def objective_sort_key(objective: ObjectiveTuple) -> tuple[Any, ...]:
    """A key that sorts BETTER candidates first under the §8 ordering."""
    return (
        -objective.should_units,
        -objective.may_units,
        objective.total_cost,
        objective.makespan,
        objective.tie_break,
    )


def canonical_schedule_serialization(
    selected: Sequence[ScheduledAction], deferred: Sequence[str]
) -> str:
    """Stage 5 of §8: the canonical serialization used as the tie-break.

    Shared between planner and checker by design — the design permits
    exactly schema parsing and canonicalization to be shared, and a
    tie-break that both sides do not compute identically is not a
    tie-break."""
    return canonical_json({
        "selected": [s.to_schedule_dict()
                     for s in sorted(selected, key=lambda s: s.action_id)],
        "deferred": sorted(deferred),
    })


@dataclass(frozen=True)
class SchedulePlan:
    """A candidate plan for one frozen epoch.

    Produced by an UNTRUSTED planner (see
    :mod:`.batch_schedule_baselines`) and validated by
    :mod:`.batch_schedule_checker`. Nothing here is believed: the
    claimed objective, the disclosure set, and the witnesses are all
    re-derived from the instance before any of them is credited."""

    schema_version: str
    instance_id: str
    selected: tuple[ScheduledAction, ...]
    deferred: tuple[str, ...]
    witnesses: tuple[CoverageWitness, ...]
    #: The MayChange disclosure set the planner claims is sufficient.
    may_change: tuple[str, ...]
    claimed_objective: ObjectiveTuple

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "instance_id": self.instance_id,
            "selected": [s.to_canonical_dict() for s in
                         sorted(self.selected, key=lambda s: s.action_id)],
            "deferred": sorted(self.deferred),
            "witnesses": [w.to_canonical_dict() for w in
                          sorted(self.witnesses,
                                 key=lambda w: (w.unit_key, w.obligation_id,
                                                w.action_id))],
            "may_change": sorted(self.may_change),
            "claimed_objective": self.claimed_objective.to_canonical_dict(),
        }

    @property
    def digest(self) -> str:
        return object_digest(self.to_canonical_dict())


def plan_digest(plan: SchedulePlan) -> str:
    return plan.digest


# --------------------------------------------------------------------------- #
# The certificate (§12) and the optimality artifact (§9.1).                    #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ProducerRef:
    """WHO produced a plan, bound into its certificate (§12).

    §12 requires every certificate to bind "solver family, version,
    parameters, random seed, and termination status". Producer identity
    is not decoration: a certificate that does not say who made the plan
    cannot be traced back to a reviewed producer, and
    ``termination_status`` is what separates a finished search from one
    that stopped early and would have no business claiming exhaustion.

    ``seed`` is a :class:`MaybeInt`: a deterministic producer states
    UNKNOWN with a reason rather than writing a fictional ``0``."""

    family: str
    version: str
    parameters_digest: str
    seed: MaybeInt
    termination_status: str

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "family": self.family,
            "version": self.version,
            "parameters_digest": self.parameters_digest,
            "seed": _quantity_to_canonical(self.seed),
            "termination_status": self.termination_status,
        }


@dataclass(frozen=True)
class ScheduleCertificate:
    """What a checked result binds (§12).

    Every field is a binding, not a claim: the checker recomputes each
    digest from the supplied instance and plan and refuses on any
    mismatch. Mutating the plan after digesting therefore fails the
    binding, and mutating the certificate to match a mutated plan fails
    the constraint checks instead — an adversary has to defeat both.

    Two bindings exist specifically so that an optimality claim cannot
    float free of the objects that support it (§12, and the EWD-BSC-7
    review finding): :attr:`producer` names who produced the plan and how
    its search terminated, and :attr:`optimality_artifact_digest` names
    the ONE artifact this certificate stands behind. Without the latter,
    any artifact could be paired with any certificate over the same
    instance."""

    schema_version: str
    instance_digest: str
    unit_mapping_digest: str
    plan_digest: str
    claimed_objective: ObjectiveTuple
    checker_version: str
    producer: ProducerRef
    #: :data:`ARTIFACT_UNBOUND`, or the digest of the ONE
    #: :class:`OptimalityArtifact` this certificate stands behind.
    optimality_artifact_digest: str

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "instance_digest": self.instance_digest,
            "unit_mapping_digest": self.unit_mapping_digest,
            "plan_digest": self.plan_digest,
            "claimed_objective": self.claimed_objective.to_canonical_dict(),
            "checker_version": self.checker_version,
            "producer": self.producer.to_canonical_dict(),
            "optimality_artifact_digest": self.optimality_artifact_digest,
        }

    @property
    def digest(self) -> str:
        return object_digest(self.to_canonical_dict())


@dataclass(frozen=True)
class OptimalityArtifact:
    """The ONLY thing that may license the word "optimal" (§9.1).

    A solver status string, a feasibility certificate, and a correctly
    recomputed objective value are each explicitly NOT optimality
    proofs. This artifact carries the exhaustive-enumeration record (or
    a solver-independent bound) whose value must equal the checked
    candidate's value at every objective stage.

    ``method`` is drawn from :data:`OPTIMALITY_METHODS`.
    ``search_space_digest`` binds exactly what was enumerated, so a
    silently truncated search cannot masquerade as an exhaustive one, and
    it is recomputable by the checker from ``(instance, method,
    max_actions, node_budget, candidates_examined)`` alone — see
    :func:`search_space_digest`. ``max_actions`` and ``node_budget`` are
    carried HERE, rather than left to the producer's private
    configuration, precisely so that the checker can redo that
    computation instead of taking the digest on faith.

    None of these fields is believed. The checker re-derives the optimum
    itself and compares; the fields exist so that a mismatch between the
    reported search and the real one is nameable."""

    method: str
    instance_digest: str
    unit_mapping_digest: str
    best_objective: ObjectiveTuple
    candidates_examined: int
    search_space_digest: str
    producer_version: str
    max_actions: int
    node_budget: int

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "method": self.method,
            "instance_digest": self.instance_digest,
            "unit_mapping_digest": self.unit_mapping_digest,
            "best_objective": self.best_objective.to_canonical_dict(),
            "candidates_examined": self.candidates_examined,
            "search_space_digest": self.search_space_digest,
            "producer_version": self.producer_version,
            "max_actions": self.max_actions,
            "node_budget": self.node_budget,
        }

    @property
    def digest(self) -> str:
        return object_digest(self.to_canonical_dict())


# --------------------------------------------------------------------------- #
# The search-space characterization (§9.1).                                    #
# --------------------------------------------------------------------------- #
# These three functions live in the TRUSTED BASE on purpose. A producer
# that computed its own `search_space_digest` privately would be binding
# a claim only it could evaluate, and the checker would have no choice
# but to echo it. Here, both sides derive the same value from the same
# canonical instance, so a producer that enumerated a different space
# than it reported is caught by arithmetic rather than by trust. They are
# characterization and canonicalization — not search: nothing here
# places an action or compares a candidate.
def enumeration_horizon(instance: BatchInstance) -> int:
    """The time bound beyond which no active schedule can extend.

    Every action run back-to-back, offset by the last capacity
    breakpoint. Refuses on an UNKNOWN duration: a horizon computed by
    reading an unknown as zero would silently shrink the space."""
    total = 0
    for action in sorted(instance.actions, key=lambda a: a.id):
        if isinstance(action.duration, Unknown):
            _refuse(
                REFUSAL_NON_CANONICAL_VALUE, f"$.actions[{action.id}].duration",
                "an UNKNOWN duration has no horizon; the search space is "
                "not characterizable and no exhaustive claim is available",
            )
        assert isinstance(action.duration, int)
        total += action.duration
    breakpoints = enumeration_breakpoints(instance)
    return total + (max(breakpoints) if breakpoints else 0)


def enumeration_breakpoints(instance: BatchInstance) -> tuple[int, ...]:
    """Every capacity breakpoint, ascending and deduplicated (§7)."""
    return tuple(sorted({
        segment.start
        for resource in instance.resources
        for segment in resource.segments
    }))


def search_space_digest(
    instance: BatchInstance,
    *,
    method: str,
    max_actions: int,
    node_budget: int,
    candidates_examined: int,
) -> str:
    """Bind exactly which space a search claims to have covered (§9.1)."""
    return object_digest({
        "method": method,
        "max_actions": max_actions,
        "node_budget": node_budget,
        "horizon": enumeration_horizon(instance),
        "breakpoints": list(enumeration_breakpoints(instance)),
        "actions": sorted(a.id for a in instance.actions),
        "candidates": candidates_examined,
    })


# --------------------------------------------------------------------------- #
# The fail-closed loaders.                                                     #
# --------------------------------------------------------------------------- #
def _require_mapping(value: Any, where: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        _refuse(REFUSAL_MISSING_FIELD, where, "expected an object")
    return value


def _fields(
    raw: Mapping[str, Any], where: str, required: Sequence[str],
    optional: Sequence[str] = (),
) -> None:
    allowed = set(required) | set(optional)
    for key in sorted(raw):
        if key not in allowed:
            _refuse(REFUSAL_UNKNOWN_FIELD, f"{where}.{key}",
                    "field is not part of the contract")
    for key in required:
        if key not in raw:
            _refuse(REFUSAL_MISSING_FIELD, f"{where}.{key}",
                    "required field is absent; nothing defaults silently")


def _load_str(raw: Mapping[str, Any], key: str, where: str) -> str:
    value = raw[key]
    if not isinstance(value, str) or not value:
        _refuse(REFUSAL_NON_CANONICAL_VALUE, f"{where}.{key}",
                "expected a non-empty string")
    return value


def _load_int(
    raw: Mapping[str, Any], key: str, where: str, *, minimum: int = 0
) -> int:
    value = raw[key]
    if isinstance(value, bool) or not isinstance(value, int):
        _refuse(REFUSAL_NON_CANONICAL_VALUE, f"{where}.{key}",
                "expected an integer in the declared base unit")
    if value < minimum:
        _refuse(REFUSAL_NON_CANONICAL_VALUE, f"{where}.{key}",
                f"expected an integer >= {minimum}")
    return value


def _load_maybe_int(value: Any, where: str, *, minimum: int = 0) -> MaybeInt:
    """An integer, or the UNKNOWN sentinel spelled ``{"unknown": reason}``."""
    if isinstance(value, Mapping):
        _fields(value, where, ["unknown"])
        reason = value["unknown"]
        if not isinstance(reason, str) or not reason:
            _refuse(REFUSAL_NON_CANONICAL_VALUE, f"{where}.unknown",
                    "an unknown must state a non-empty reason")
        return Unknown(reason)
    if isinstance(value, bool) or not isinstance(value, int):
        _refuse(
            REFUSAL_NON_CANONICAL_VALUE, where,
            "expected an integer or the UNKNOWN sentinel "
            '{"unknown": "<reason>"}; an absent quantity is never zero',
        )
    if value < minimum:
        _refuse(REFUSAL_NON_CANONICAL_VALUE, where,
                f"expected an integer >= {minimum}")
    return value


def _load_str_tuple(
    raw: Mapping[str, Any], key: str, where: str
) -> tuple[str, ...]:
    value = raw[key]
    if not isinstance(value, (list, tuple)):
        _refuse(REFUSAL_NON_CANONICAL_VALUE, f"{where}.{key}",
                "expected a list of strings")
    out: list[str] = []
    for i, item in enumerate(value):
        if not isinstance(item, str) or not item:
            _refuse(REFUSAL_NON_CANONICAL_VALUE, f"{where}.{key}[{i}]",
                    "expected a non-empty string")
        if item in out:
            _refuse(REFUSAL_DUPLICATE_IDENTITY, f"{where}.{key}[{i}]",
                    f"identity {item!r} is listed twice")
        out.append(item)
    return tuple(out)


def _load_coverage_unit(raw: Any, where: str) -> CoverageUnit:
    raw = _require_mapping(raw, where)
    _fields(raw, where,
            ["spec_version", "subject_scope_key", "satisfaction_condition"])
    return CoverageUnit(
        spec_version=_load_str(raw, "spec_version", where),
        subject_scope_key=_load_str(raw, "subject_scope_key", where),
        satisfaction_condition=_load_str(raw, "satisfaction_condition", where),
    )


def _load_reveals(raw: Any, where: str) -> Revelation | Unknown:
    raw = _require_mapping(raw, where)
    if "unknown" in raw:
        _fields(raw, where, ["unknown"])
        reason = raw["unknown"]
        if not isinstance(reason, str) or not reason:
            _refuse(REFUSAL_NON_CANONICAL_VALUE, f"{where}.unknown",
                    "an unknown revelation must state a non-empty reason")
        return Unknown(reason)
    _fields(raw, where, ["obligation_classes", "action_classes"])
    return Revelation(
        obligation_classes=_load_str_tuple(raw, "obligation_classes", where),
        action_classes=_load_str_tuple(raw, "action_classes", where),
    )


def _load_effects(raw: Any, where: str) -> Effects | Unknown:
    raw = _require_mapping(raw, where)
    if "unknown" in raw:
        _fields(raw, where, ["unknown"])
        reason = raw["unknown"]
        if not isinstance(reason, str) or not reason:
            _refuse(REFUSAL_NON_CANONICAL_VALUE, f"{where}.unknown",
                    "an unknown effect must state a non-empty reason")
        return Unknown(reason)
    _fields(raw, where, ["locations"])
    return Effects(locations=_load_str_tuple(raw, "locations", where))


def load_batch_instance(raw: Any) -> BatchInstance:
    """Parse and validate one frozen batch instance, fail-closed.

    Refuses on an unknown field, a missing field, a dangling reference,
    a duplicate identity, an unmapped priority, or an action whose cost
    unit differs from the instance's single declared unit. It does NOT
    check acyclicity or any plan property — those are the checker's
    duties (§9), and keeping them there is what makes the checker's
    rejecting tests reachable."""
    where = "$"
    raw = _require_mapping(raw, where)
    _fields(raw, where, [
        "schema_version", "instance_id", "epoch", "cost_unit",
        "unit_mapping_version", "changes", "obligations", "actions",
        "conflict_groups", "resources",
    ])
    schema_version = _load_str(raw, "schema_version", where)
    if schema_version != CONTRACT_SCHEMA_VERSION:
        _refuse(REFUSAL_SCHEMA_VERSION, f"{where}.schema_version",
                f"expected {CONTRACT_SCHEMA_VERSION!r}, got "
                f"{schema_version!r}")
    cost_unit = _load_str(raw, "cost_unit", where)

    changes = tuple(
        _load_change(item, f"{where}.changes[{i}]")
        for i, item in enumerate(_as_list(raw["changes"], f"{where}.changes"))
    )
    _no_duplicate_ids([c.id for c in changes], f"{where}.changes")

    obligations = tuple(
        _load_obligation(item, f"{where}.obligations[{i}]")
        for i, item in enumerate(
            _as_list(raw["obligations"], f"{where}.obligations"))
    )
    _no_duplicate_ids([o.id for o in obligations], f"{where}.obligations")
    _check_unit_priority_agreement(obligations)

    actions = tuple(
        _load_action(item, f"{where}.actions[{i}]", cost_unit)
        for i, item in enumerate(_as_list(raw["actions"], f"{where}.actions"))
    )
    _no_duplicate_ids([a.id for a in actions], f"{where}.actions")

    conflict_groups = tuple(
        _load_conflict_group(item, f"{where}.conflict_groups[{i}]")
        for i, item in enumerate(
            _as_list(raw["conflict_groups"], f"{where}.conflict_groups"))
    )
    _no_duplicate_ids([g.id for g in conflict_groups],
                      f"{where}.conflict_groups")

    resources = tuple(
        _load_resource(item, f"{where}.resources[{i}]")
        for i, item in enumerate(
            _as_list(raw["resources"], f"{where}.resources"))
    )
    _no_duplicate_ids([r.id for r in resources], f"{where}.resources")

    instance = BatchInstance(
        schema_version=schema_version,
        instance_id=_load_str(raw, "instance_id", where),
        epoch=_load_int(raw, "epoch", where),
        cost_unit=cost_unit,
        unit_mapping_version=_load_str(raw, "unit_mapping_version", where),
        changes=changes,
        obligations=obligations,
        actions=actions,
        conflict_groups=conflict_groups,
        resources=resources,
    )
    _check_references(instance)
    return instance


def _as_list(value: Any, where: str) -> Sequence[Any]:
    if not isinstance(value, (list, tuple)):
        _refuse(REFUSAL_NON_CANONICAL_VALUE, where, "expected a list")
    return value


def _no_duplicate_ids(ids: Sequence[str], where: str) -> None:
    seen: set[str] = set()
    for identity in ids:
        if identity in seen:
            _refuse(REFUSAL_DUPLICATE_IDENTITY, where,
                    f"identity {identity!r} is declared twice")
        seen.add(identity)


def _load_change(raw: Any, where: str) -> Change:
    raw = _require_mapping(raw, where)
    _fields(raw, where, ["id", "affected_scope"])
    scope = _load_str_tuple(raw, "affected_scope", where)
    if not scope:
        _refuse(REFUSAL_EMPTY_DECLARATION, f"{where}.affected_scope",
                "a change with no affected scope cannot bound disclosure; "
                "declare the scope explicitly")
    return Change(id=_load_str(raw, "id", where), affected_scope=scope)


def _load_obligation(raw: Any, where: str) -> Obligation:
    raw = _require_mapping(raw, where)
    _fields(raw, where, ["id", "obligation_class", "priority", "unit"])
    priority = raw["priority"]
    if not isinstance(priority, str) or priority not in PRIORITIES:
        _refuse(
            REFUSAL_UNKNOWN_PRIORITY, f"{where}.priority",
            f"priority {priority!r} is not one of {PRIORITIES}; an "
            "unmapped policy REFUSES — the loader never guesses whether "
            "work is mandatory (design §6)",
        )
    return Obligation(
        id=_load_str(raw, "id", where),
        obligation_class=_load_str(raw, "obligation_class", where),
        priority=priority,
        unit=_load_coverage_unit(raw["unit"], f"{where}.unit"),
    )


def _check_unit_priority_agreement(
    obligations: Sequence[Obligation],
) -> None:
    """Priority attaches to the UNIT (§6), so the mapping must agree.

    Two obligations that collapse to one coverage unit but declare
    different priorities make "is this unit mandatory?" unanswerable.
    Guessing either way is wrong, so it refuses."""
    seen: dict[str, tuple[str, str]] = {}
    for obligation in obligations:
        key = obligation.unit.key
        if key in seen:
            priority, first_id = seen[key]
            if priority != obligation.priority:
                _refuse(
                    REFUSAL_CONFLICTING_UNIT_PRIORITY,
                    f"$.obligations[{obligation.id}]",
                    f"obligations {first_id!r} and {obligation.id!r} map to "
                    f"the same coverage unit but declare priorities "
                    f"{priority!r} and {obligation.priority!r}",
                )
        else:
            seen[key] = (obligation.priority, obligation.id)


def _load_action(raw: Any, where: str, instance_cost_unit: str) -> Action:
    raw = _require_mapping(raw, where)
    _fields(raw, where, [
        "id", "action_class", "covers", "requires", "conflicts", "reveals",
        "effects", "duration", "execution_cost", "cost_unit", "demand",
        "estimator",
    ])
    cost_unit = _load_str(raw, "cost_unit", where)
    if cost_unit != instance_cost_unit:
        _refuse(
            REFUSAL_MIXED_COST_UNITS, f"{where}.cost_unit",
            f"action declares cost unit {cost_unit!r} but the instance "
            f"declares {instance_cost_unit!r}; unlike units may not be "
            "added (design §7.1) — declare a reviewed lexicographic cost "
            "vector instead",
        )
    demand_raw = _require_mapping(raw["demand"], f"{where}.demand")
    demand = {
        resource: _load_maybe_int(value, f"{where}.demand.{resource}")
        for resource, value in sorted(demand_raw.items())
    }
    estimator_raw = _require_mapping(raw["estimator"], f"{where}.estimator")
    _fields(estimator_raw, f"{where}.estimator", ["id", "version"])
    duration = _load_maybe_int(raw["duration"], f"{where}.duration")
    if duration == 0:
        _refuse(
            REFUSAL_NON_CANONICAL_VALUE, f"{where}.duration",
            "a non-preemptive action OCCUPIES time (design §7): a "
            "zero-length interval satisfies no `start <= t < end`, so the "
            "action would be invisible to every capacity, conflict, "
            "concurrency, and reservation check while still earning its "
            "coverage credit. An unmeasured duration is spelled UNKNOWN; "
            "there is no zero-length chunk in the model",
        )
    return Action(
        id=_load_str(raw, "id", where),
        action_class=_load_str(raw, "action_class", where),
        covers=_load_str_tuple(raw, "covers", where),
        requires=_load_str_tuple(raw, "requires", where),
        conflicts=_load_str_tuple(raw, "conflicts", where),
        reveals=_load_reveals(raw["reveals"], f"{where}.reveals"),
        effects=_load_effects(raw["effects"], f"{where}.effects"),
        duration=duration,
        execution_cost=_load_maybe_int(raw["execution_cost"],
                                       f"{where}.execution_cost"),
        cost_unit=cost_unit,
        demand=demand,
        estimator=EstimatorRef(
            id=_load_str(estimator_raw, "id", f"{where}.estimator"),
            version=_load_int(estimator_raw, "version", f"{where}.estimator"),
        ),
    )


def _load_conflict_group(raw: Any, where: str) -> ConflictGroup:
    raw = _require_mapping(raw, where)
    _fields(raw, where, ["id", "members", "max_concurrent"])
    members = _load_str_tuple(raw, "members", where)
    if len(members) < 2:
        _refuse(REFUSAL_EMPTY_DECLARATION, f"{where}.members",
                "a conflict group constrains at least two actions")
    return ConflictGroup(
        id=_load_str(raw, "id", where),
        members=members,
        max_concurrent=_load_int(raw, "max_concurrent", where, minimum=1),
    )


def _load_resource(raw: Any, where: str) -> Resource:
    raw = _require_mapping(raw, where)
    _fields(raw, where,
            ["id", "version", "segments", "reservation", "hard_cap"])
    segments_raw = _as_list(raw["segments"], f"{where}.segments")
    if not segments_raw:
        _refuse(REFUSAL_EMPTY_DECLARATION, f"{where}.segments",
                "a resource declares at least one capacity segment")
    segments: list[CapacitySegment] = []
    for i, item in enumerate(segments_raw):
        seg_where = f"{where}.segments[{i}]"
        item = _require_mapping(item, seg_where)
        _fields(item, seg_where, ["start", "capacity"])
        segments.append(CapacitySegment(
            start=_load_int(item, "start", seg_where),
            capacity=_load_maybe_int(item["capacity"], f"{seg_where}.capacity"),
        ))
    if segments[0].start != 0:
        _refuse(REFUSAL_NON_CANONICAL_VALUE, f"{where}.segments[0].start",
                "the first capacity segment starts at time 0")
    for earlier, later in zip(segments, segments[1:]):
        if later.start <= earlier.start:
            _refuse(REFUSAL_NON_CANONICAL_VALUE, f"{where}.segments",
                    "capacity breakpoints are strictly ascending")
    return Resource(
        id=_load_str(raw, "id", where),
        version=_load_int(raw, "version", where),
        segments=tuple(segments),
        reservation=_load_maybe_int(raw["reservation"],
                                    f"{where}.reservation"),
        hard_cap=_load_maybe_int(raw["hard_cap"], f"{where}.hard_cap"),
    )


def _check_references(instance: BatchInstance) -> None:
    action_ids = {a.id for a in instance.actions}
    obligation_ids = {o.id for o in instance.obligations}
    resource_ids = {r.id for r in instance.resources}
    for action in instance.actions:
        where = f"$.actions[{action.id}]"
        for obligation_id in action.covers:
            if obligation_id not in obligation_ids:
                _refuse(REFUSAL_DANGLING_REFERENCE, f"{where}.covers",
                        f"unknown obligation {obligation_id!r}")
        for other in action.requires:
            if other not in action_ids:
                _refuse(REFUSAL_DANGLING_REFERENCE, f"{where}.requires",
                        f"unknown action {other!r}")
            if other == action.id:
                _refuse(REFUSAL_DANGLING_REFERENCE, f"{where}.requires",
                        "an action cannot be its own prerequisite")
        for other in action.conflicts:
            if other not in action_ids:
                _refuse(REFUSAL_DANGLING_REFERENCE, f"{where}.conflicts",
                        f"unknown action {other!r}")
        for resource_id in action.demand:
            if resource_id not in resource_ids:
                _refuse(REFUSAL_DANGLING_REFERENCE, f"{where}.demand",
                        f"unknown resource {resource_id!r}")
        # §7 declares `demand(a, r)` FOR EACH named resource `r`. An
        # omitted key is not "no demand": it is an unanswered question,
        # and reading it as zero is exactly the permissive default the
        # UNKNOWN sentinel exists to prevent — it converts a capacity
        # violation into a clean pass, with nothing in `not_established`
        # to show for it. Zero demand stays perfectly expressible; it
        # just has to be WRITTEN.
        for resource_id in sorted(resource_ids):
            if resource_id not in action.demand:
                _refuse(
                    REFUSAL_MISSING_FIELD, f"{where}.demand.{resource_id}",
                    f"the instance declares resource {resource_id!r} and "
                    "this action's demand mapping is silent about it; "
                    "write 0 for no demand, or "
                    '{"unknown": "<reason>"} for an unmeasured one — an '
                    "absent key is never a zero (design §7)",
                )
    for group in instance.conflict_groups:
        for member in group.members:
            if member not in action_ids:
                _refuse(REFUSAL_DANGLING_REFERENCE,
                        f"$.conflict_groups[{group.id}].members",
                        f"unknown action {member!r}")


def load_plan(raw: Any) -> SchedulePlan:
    """Parse a candidate plan, fail-closed. Nothing here is believed."""
    where = "$"
    raw = _require_mapping(raw, where)
    _fields(raw, where, [
        "schema_version", "instance_id", "selected", "deferred", "witnesses",
        "may_change", "claimed_objective",
    ])
    schema_version = _load_str(raw, "schema_version", where)
    if schema_version != CONTRACT_SCHEMA_VERSION:
        _refuse(REFUSAL_SCHEMA_VERSION, f"{where}.schema_version",
                f"expected {CONTRACT_SCHEMA_VERSION!r}")
    selected: list[ScheduledAction] = []
    for i, item in enumerate(_as_list(raw["selected"], f"{where}.selected")):
        sel_where = f"{where}.selected[{i}]"
        item = _require_mapping(item, sel_where)
        _fields(item, sel_where,
                ["action_id", "start", "end", "resource_assignments",
                 "outcome"])
        outcome = item["outcome"]
        if not isinstance(outcome, str) or outcome not in OUTCOMES:
            _refuse(REFUSAL_NON_CANONICAL_VALUE, f"{sel_where}.outcome",
                    f"outcome {outcome!r} is not one of {OUTCOMES}; a "
                    "selected action reports what happened to it, and an "
                    "unreported outcome is `unknown`, never a pass")
        assignments_raw = _require_mapping(
            item["resource_assignments"], f"{sel_where}.resource_assignments")
        assignments = {}
        for resource_id, value in sorted(assignments_raw.items()):
            aw = f"{sel_where}.resource_assignments.{resource_id}"
            if isinstance(value, bool) or not isinstance(value, int):
                _refuse(REFUSAL_NON_CANONICAL_VALUE, aw,
                        "a resource assignment is an integer; an UNKNOWN "
                        "assignment is not an assignment")
            if value < 0:
                _refuse(REFUSAL_NON_CANONICAL_VALUE, aw, "expected >= 0")
            assignments[resource_id] = value
        selected.append(ScheduledAction(
            action_id=_load_str(item, "action_id", sel_where),
            start=_load_int(item, "start", sel_where),
            end=_load_int(item, "end", sel_where),
            resource_assignments=assignments,
            outcome=outcome,
        ))
    _no_duplicate_ids([s.action_id for s in selected], f"{where}.selected")

    witnesses: list[CoverageWitness] = []
    for i, item in enumerate(_as_list(raw["witnesses"], f"{where}.witnesses")):
        wit_where = f"{where}.witnesses[{i}]"
        item = _require_mapping(item, wit_where)
        _fields(item, wit_where,
                ["unit_key", "obligation_id", "action_id", "outcome"])
        outcome = item["outcome"]
        if not isinstance(outcome, str) or outcome not in OUTCOMES:
            _refuse(REFUSAL_NON_CANONICAL_VALUE, f"{wit_where}.outcome",
                    f"outcome {outcome!r} is not one of {OUTCOMES}")
        witnesses.append(CoverageWitness(
            unit_key=_load_str(item, "unit_key", wit_where),
            obligation_id=_load_str(item, "obligation_id", wit_where),
            action_id=_load_str(item, "action_id", wit_where),
            outcome=outcome,
        ))

    objective_raw = _require_mapping(raw["claimed_objective"],
                                     f"{where}.claimed_objective")
    return SchedulePlan(
        schema_version=schema_version,
        instance_id=_load_str(raw, "instance_id", where),
        selected=tuple(selected),
        deferred=_load_str_tuple(raw, "deferred", where),
        witnesses=tuple(witnesses),
        may_change=_load_str_tuple(raw, "may_change", where),
        claimed_objective=_load_objective(objective_raw,
                                          f"{where}.claimed_objective"),
    )


def _load_objective(raw: Mapping[str, Any], where: str) -> ObjectiveTuple:
    _fields(raw, where, ["should_units", "may_units", "total_cost",
                         "makespan", "tie_break"])
    tie_break = raw["tie_break"]
    if not isinstance(tie_break, str):
        _refuse(REFUSAL_NON_CANONICAL_VALUE, f"{where}.tie_break",
                "expected the canonical schedule serialization")
    return ObjectiveTuple(
        should_units=_load_int(raw, "should_units", where),
        may_units=_load_int(raw, "may_units", where),
        total_cost=_load_int(raw, "total_cost", where),
        makespan=_load_int(raw, "makespan", where),
        tie_break=tie_break,
    )


def load_certificate(raw: Any) -> ScheduleCertificate:
    """Parse a schedule certificate, fail-closed."""
    where = "$"
    raw = _require_mapping(raw, where)
    _fields(raw, where, [
        "schema_version", "instance_digest", "unit_mapping_digest",
        "plan_digest", "claimed_objective", "checker_version", "producer",
        "optimality_artifact_digest",
    ])
    schema_version = _load_str(raw, "schema_version", where)
    if schema_version != CONTRACT_SCHEMA_VERSION:
        _refuse(REFUSAL_SCHEMA_VERSION, f"{where}.schema_version",
                f"expected {CONTRACT_SCHEMA_VERSION!r}")
    objective_raw = _require_mapping(raw["claimed_objective"],
                                     f"{where}.claimed_objective")
    return ScheduleCertificate(
        schema_version=schema_version,
        instance_digest=_load_str(raw, "instance_digest", where),
        unit_mapping_digest=_load_str(raw, "unit_mapping_digest", where),
        plan_digest=_load_str(raw, "plan_digest", where),
        claimed_objective=_load_objective(objective_raw,
                                          f"{where}.claimed_objective"),
        checker_version=_load_str(raw, "checker_version", where),
        producer=_load_producer(raw["producer"], f"{where}.producer"),
        optimality_artifact_digest=_load_str(
            raw, "optimality_artifact_digest", where),
    )


def _load_producer(raw: Any, where: str) -> ProducerRef:
    raw = _require_mapping(raw, where)
    _fields(raw, where, ["family", "version", "parameters_digest", "seed",
                         "termination_status"])
    status = raw["termination_status"]
    if not isinstance(status, str) or status not in TERMINATION_STATUSES:
        _refuse(REFUSAL_NON_CANONICAL_VALUE, f"{where}.termination_status",
                f"termination status {status!r} is not one of "
                f"{TERMINATION_STATUSES}; a solver status string is not a "
                "vocabulary (design §9.1)")
    return ProducerRef(
        family=_load_str(raw, "family", where),
        version=_load_str(raw, "version", where),
        parameters_digest=_load_str(raw, "parameters_digest", where),
        seed=_load_maybe_int(raw["seed"], f"{where}.seed"),
        termination_status=status,
    )
