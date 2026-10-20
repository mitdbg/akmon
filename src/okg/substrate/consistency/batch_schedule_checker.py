"""The independent batch-schedule checker: feasibility and value ONLY.

PACT ``concord-disclosure-budget``, task.batch.contract-checker
(req.batch.checker-implementation), step 2 of
``designs/batch-scheduling-research.md`` §13. Normative sections: §9
(the solver and certificate boundary), §9.1 (the optimality rule), §8
(the lexicographic objective), §7 (the batch model), §6 (priority and
coverage units). Parent design ``concord-invariant-engine`` v8 §6,
"What is enforced versus what is aspired".

The independence contract
-------------------------
This module imports :mod:`.batch_schedule` — the declared trusted base
of schema, parsing, and canonicalization — plus the standard library
and :class:`SubstrateError`. **Nothing else.** It must never import a
planner, a baseline, a solver encoding, or the oracle. That direction is
enforced by a test that inspects the import graph, not by review, because
a checker that shares a search routine with the thing it checks proves
nothing about that thing.

The checker re-derives every constraint and every objective component
from the canonical instance. It reads the plan and the certificate only
as CLAIMS to be refuted.

What a passing check establishes
--------------------------------
Exactly two things, on the supplied instance:

* **Feasibility** — the identity partition is complete, prerequisite
  closure holds, every prerequisite SUCCEEDED before its dependent
  started, the dependency graph is acyclic, pairwise and grouped
  conflicts are respected, every named capacity holds at every action
  start, every action end, AND every capacity breakpoint, reservations
  and caps hold **wherever they are enforceable** — a bound that cannot
  be enforced is listed in
  :attr:`ScheduleCheckResult.not_established`, never passed — every
  known ``must`` coverage unit is discharged by a successful witness on
  a successful action, and the claimed disclosure set contains the
  conservative ``MayChange`` closure.
* **Value** — the claimed lexicographic tuple equals the recomputed one,
  including the deterministic tie-break.

What it does NOT establish
--------------------------
Dependency completeness, effect-summary soundness, check correctness,
extraction completeness, factual truth, future revelations, or
**optimality** (§9). The verdict vocabulary deliberately contains no
"optimal" token and :class:`ScheduleCheckResult` has no field that could
carry one: a checked feasible candidate with a checked value is the most
this module can ever say. Optimality is expressible only through
:func:`check_optimality_artifact`, which REQUIRES an
:class:`~.batch_schedule.OptimalityArtifact` (§9.1). A solver status
string, a feasibility certificate, and a correctly recomputed objective
value are each explicitly not optimality proofs — and neither is an
artifact that agrees with its own candidate, so
:func:`check_optimality_artifact` RE-DERIVES the optimum from the
instance and compares, within a bound it declares for itself and
refuses beyond.

Fail-closed asymmetries worth naming
------------------------------------
* An UNKNOWN duration, cost, demand, or in-use capacity does not become
  zero — it makes the dependent check unestablishable, and the plan is
  refused with a cause that names which quantity was missing. An
  OMITTED demand key reads as UNKNOWN for the same reason: silence
  about a declared resource is not a claim of zero demand.
* A prerequisite that did not report ``success`` licenses nothing.
  Coverage credit stops at the first failure in a chain rather than
  flowing past it.
* An UNKNOWN reservation or hard cap is different in kind: it is a
  BUDGET bound, and design §7 says an unknown bound cannot support a
  hard-budget guarantee. Those appear in
  :attr:`ScheduleCheckResult.not_established` — reported, never quietly
  passed and never silently upgraded to a guarantee.
* An UNKNOWN effect widens ``MayChange`` to the whole affected scope.
* An UNKNOWN revelation anywhere in the deferred closure is treated as
  able to reveal an applicable ``must``, and qualified publication is
  REFUSED. This is deliberately stricter than disclosure widening: a
  hidden mandatory obligation cannot be repaired by a warning.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, Mapping, Sequence

from okg.substrate.consistency.batch_schedule import (
    ARTIFACT_UNBOUND,
    CONTRACT_SCHEMA_VERSION,
    OPTIMALITY_METHOD_EXHAUSTIVE,
    OPTIMALITY_METHODS,
    OUTCOME_SUCCESS,
    PRIORITY_MAY,
    PRIORITY_MUST,
    PRIORITY_SHOULD,
    TERMINATION_EXHAUSTED,
    TERMINATION_STATUSES,
    TERMINATION_STOPPED,
    Action,
    BatchInstance,
    BatchLoadRefused,
    CoverageWitness,
    Effects,
    ObjectiveTuple,
    OptimalityArtifact,
    ProducerRef,
    Revelation,
    ScheduleCertificate,
    ScheduledAction,
    SchedulePlan,
    Unknown,
    canonical_json,
    canonical_schedule_serialization,
    enumeration_breakpoints,
    enumeration_horizon,
    object_digest,
    objective_sort_key,
    search_space_digest,
)

__all__ = [
    "CAUSES",
    "CAUSE_ARTIFACT_CANDIDATE_NOT_OPTIMUM",
    "CAUSE_ARTIFACT_DIGEST_MISMATCH",
    "CAUSE_ARTIFACT_METHOD_UNKNOWN",
    "CAUSE_ARTIFACT_NOT_BOUND",
    "CAUSE_ARTIFACT_PRODUCER_UNREVIEWED",
    "CAUSE_ARTIFACT_REQUIRES_FEASIBLE_CHECK",
    "CAUSE_ARTIFACT_SEARCH_MISMATCH",
    "CAUSE_ARTIFACT_SEARCH_UNVERIFIABLE",
    "CAUSE_ARTIFACT_UNESTABLISHED_BOUND",
    "CAUSE_ARTIFACT_VALUE_MISMATCH",
    "CAUSE_CAPACITY_EXCEEDED",
    "CAUSE_CAPACITY_UNKNOWN",
    "CAUSE_CERTIFICATE_OBJECTIVE_MISMATCH",
    "CAUSE_CONFLICT_GROUP_VIOLATION",
    "CAUSE_CONFLICT_VIOLATION",
    "CAUSE_COST_UNKNOWN",
    "CAUSE_DEFERRED_MAY_REVEAL_MUST",
    "CAUSE_DEMAND_UNKNOWN",
    "CAUSE_DEPENDENCY_CYCLE",
    "CAUSE_DURATION_MISMATCH",
    "CAUSE_DURATION_UNKNOWN",
    "CAUSE_HARD_CAP_EXCEEDED",
    "CAUSE_IDENTITY_PARTITION",
    "CAUSE_INSTANCE_DIGEST_MISMATCH",
    "CAUSE_MANDATORY_UNIT_UNCOVERED",
    "CAUSE_MAY_CHANGE_INCOMPLETE",
    "CAUSE_OBJECTIVE_MISMATCH",
    "CAUSE_PLAN_DIGEST_MISMATCH",
    "CAUSE_PREREQUISITE_NOT_SELECTED",
    "CAUSE_PREREQUISITE_NOT_SUCCEEDED",
    "CAUSE_PRODUCER_BINDING_INVALID",
    "CAUSE_RESERVATION_EXCEEDED",
    "CAUSE_RESOURCE_ASSIGNMENT_MISMATCH",
    "CAUSE_SCHEMA_VERSION_MISMATCH",
    "CAUSE_TEMPORAL_ORDER_VIOLATION",
    "CAUSE_UNIT_MAPPING_DIGEST_MISMATCH",
    "CAUSE_UNKNOWN_REVELATION",
    "CAUSE_WITNESS_INVALID",
    "CHECKER_VERSION",
    "OPTIMALITY_RECHECK_MAX_ACTIONS",
    "OPTIMALITY_RECHECK_NODE_BUDGET",
    "REVIEWED_OPTIMALITY_PRODUCERS",
    "VERDICTS",
    "VERDICT_CHECKED_FEASIBLE",
    "VERDICT_REFUSED",
    "CheckFinding",
    "MayChangeDerivation",
    "OptimalityVerdict",
    "ScheduleCheckResult",
    "check_optimality_artifact",
    "check_schedule",
    "derive_may_change",
    "recompute_objective",
]


#: Bound into every certificate (§12). A checker change is a checker
#: version change; a certificate that names a different checker is not
#: this checker's certificate.
CHECKER_VERSION = "batch-schedule-checker/1"

#: The checker's OWN bound on re-deriving an optimum (§9.1). It is the
#: checker's willingness, not the artifact's: an artifact declaring a
#: larger search does not extend it, it fails against it. Beyond this
#: bound the checker refuses the optimality claim outright, which is
#: §9.1's stated fallback — "production-size results are reported as
#: checked feasible candidates with checked values and empirical gaps
#: only".
OPTIMALITY_RECHECK_MAX_ACTIONS = 6
OPTIMALITY_RECHECK_NODE_BUDGET = 200_000

#: Producer versions whose exhaustive enumeration has been reviewed. An
#: artifact from anything else is not refused because the producer is
#: presumed dishonest — it is refused because "exhaustive" is a claim
#: about a search nobody here has read.
REVIEWED_OPTIMALITY_PRODUCERS = ("batch-schedule-oracle/1",)


# --------------------------------------------------------------------------- #
# The closed verdict and cause vocabularies.                                   #
# --------------------------------------------------------------------------- #
#: The candidate is feasible on the supplied instance and its claimed
#: value is the recomputed value. NOT a claim of optimality (§9.1).
VERDICT_CHECKED_FEASIBLE = "checked_feasible"
VERDICT_REFUSED = "refused"
VERDICTS = (VERDICT_CHECKED_FEASIBLE, VERDICT_REFUSED)

CAUSE_SCHEMA_VERSION_MISMATCH = "schema_version_mismatch"
CAUSE_INSTANCE_DIGEST_MISMATCH = "instance_digest_mismatch"
CAUSE_UNIT_MAPPING_DIGEST_MISMATCH = "unit_mapping_digest_mismatch"
CAUSE_PLAN_DIGEST_MISMATCH = "plan_digest_mismatch"
CAUSE_CERTIFICATE_OBJECTIVE_MISMATCH = "certificate_objective_mismatch"
CAUSE_IDENTITY_PARTITION = "identity_partition_invalid"
CAUSE_DEPENDENCY_CYCLE = "dependency_cycle"
CAUSE_PREREQUISITE_NOT_SELECTED = "prerequisite_not_selected"
CAUSE_TEMPORAL_ORDER_VIOLATION = "temporal_order_violation"
CAUSE_DURATION_MISMATCH = "duration_mismatch"
CAUSE_DURATION_UNKNOWN = "duration_unknown"
CAUSE_COST_UNKNOWN = "cost_unknown"
CAUSE_DEMAND_UNKNOWN = "demand_unknown"
CAUSE_CONFLICT_VIOLATION = "conflict_violation"
CAUSE_CONFLICT_GROUP_VIOLATION = "conflict_group_violation"
CAUSE_CAPACITY_EXCEEDED = "capacity_exceeded"
CAUSE_CAPACITY_UNKNOWN = "capacity_unknown"
CAUSE_HARD_CAP_EXCEEDED = "hard_cap_exceeded"
CAUSE_RESERVATION_EXCEEDED = "reservation_exceeded"
CAUSE_RESOURCE_ASSIGNMENT_MISMATCH = "resource_assignment_mismatch"
CAUSE_WITNESS_INVALID = "witness_invalid"
CAUSE_MANDATORY_UNIT_UNCOVERED = "mandatory_unit_uncovered"
CAUSE_OBJECTIVE_MISMATCH = "objective_mismatch"
CAUSE_MAY_CHANGE_INCOMPLETE = "may_change_incomplete"
CAUSE_UNKNOWN_REVELATION = "unknown_revelation_refuses_publication"
CAUSE_DEFERRED_MAY_REVEAL_MUST = "deferred_may_reveal_must"
CAUSE_PREREQUISITE_NOT_SUCCEEDED = "prerequisite_not_succeeded"
CAUSE_PRODUCER_BINDING_INVALID = "producer_binding_invalid"
CAUSE_ARTIFACT_METHOD_UNKNOWN = "optimality_artifact_method_unknown"
CAUSE_ARTIFACT_DIGEST_MISMATCH = "optimality_artifact_digest_mismatch"
CAUSE_ARTIFACT_VALUE_MISMATCH = "optimality_artifact_value_mismatch"
CAUSE_ARTIFACT_REQUIRES_FEASIBLE_CHECK = (
    "optimality_artifact_requires_feasible_check"
)
#: The artifact is not the one this certificate stands behind (§12).
CAUSE_ARTIFACT_NOT_BOUND = "optimality_artifact_not_bound_by_certificate"
#: The candidate's own checked value is not the recomputed optimum.
CAUSE_ARTIFACT_CANDIDATE_NOT_OPTIMUM = "optimality_candidate_not_optimum"
#: The reported search record does not match the recomputed one.
CAUSE_ARTIFACT_SEARCH_MISMATCH = "optimality_search_record_mismatch"
#: The checker cannot redo this search, so it will not license the word.
CAUSE_ARTIFACT_SEARCH_UNVERIFIABLE = "optimality_search_unverifiable"
#: The artifact names a producer no reviewed version list contains.
CAUSE_ARTIFACT_PRODUCER_UNREVIEWED = "optimality_producer_unreviewed"
#: A guarantee the instance cannot support reached the optimality claim.
CAUSE_ARTIFACT_UNESTABLISHED_BOUND = "optimality_requires_established_bounds"

CAUSES = (
    CAUSE_ARTIFACT_CANDIDATE_NOT_OPTIMUM,
    CAUSE_ARTIFACT_DIGEST_MISMATCH,
    CAUSE_ARTIFACT_METHOD_UNKNOWN,
    CAUSE_ARTIFACT_NOT_BOUND,
    CAUSE_ARTIFACT_PRODUCER_UNREVIEWED,
    CAUSE_ARTIFACT_REQUIRES_FEASIBLE_CHECK,
    CAUSE_ARTIFACT_SEARCH_MISMATCH,
    CAUSE_ARTIFACT_SEARCH_UNVERIFIABLE,
    CAUSE_ARTIFACT_UNESTABLISHED_BOUND,
    CAUSE_ARTIFACT_VALUE_MISMATCH,
    CAUSE_CAPACITY_EXCEEDED,
    CAUSE_CAPACITY_UNKNOWN,
    CAUSE_CERTIFICATE_OBJECTIVE_MISMATCH,
    CAUSE_CONFLICT_GROUP_VIOLATION,
    CAUSE_CONFLICT_VIOLATION,
    CAUSE_COST_UNKNOWN,
    CAUSE_DEFERRED_MAY_REVEAL_MUST,
    CAUSE_DEMAND_UNKNOWN,
    CAUSE_DEPENDENCY_CYCLE,
    CAUSE_DURATION_MISMATCH,
    CAUSE_DURATION_UNKNOWN,
    CAUSE_HARD_CAP_EXCEEDED,
    CAUSE_IDENTITY_PARTITION,
    CAUSE_INSTANCE_DIGEST_MISMATCH,
    CAUSE_MANDATORY_UNIT_UNCOVERED,
    CAUSE_MAY_CHANGE_INCOMPLETE,
    CAUSE_OBJECTIVE_MISMATCH,
    CAUSE_PLAN_DIGEST_MISMATCH,
    CAUSE_PREREQUISITE_NOT_SELECTED,
    CAUSE_PREREQUISITE_NOT_SUCCEEDED,
    CAUSE_PRODUCER_BINDING_INVALID,
    CAUSE_RESERVATION_EXCEEDED,
    CAUSE_RESOURCE_ASSIGNMENT_MISMATCH,
    CAUSE_SCHEMA_VERSION_MISMATCH,
    CAUSE_TEMPORAL_ORDER_VIOLATION,
    CAUSE_UNIT_MAPPING_DIGEST_MISMATCH,
    CAUSE_UNKNOWN_REVELATION,
    CAUSE_WITNESS_INVALID,
)


@dataclass(frozen=True)
class CheckFinding:
    """One named refutation. ``cause`` is drawn from :data:`CAUSES`."""

    cause: str
    where: str
    detail: str
    data: Mapping[str, Any] = field(default_factory=dict)

    def to_canonical_dict(self) -> dict[str, Any]:
        return {"cause": self.cause, "where": self.where,
                "detail": self.detail}


@dataclass(frozen=True)
class ScheduleCheckResult:
    """The typed verdict.

    There is deliberately NO ``optimal`` field, and ``verdict`` has no
    "optimal" member. The strongest sentence this object can support is
    "checked feasible, with checked value" (§9, and program design v8
    §6: production plans are never reported as best). To say more, a
    caller must obtain an :class:`OptimalityVerdict`, which cannot be
    constructed without an oracle or bound artifact."""

    verdict: str
    findings: tuple[CheckFinding, ...]
    #: The checker's OWN recomputation, present whenever every quantity
    #: it needed was known. ``None`` means a quantity was UNKNOWN — not
    #: that the value was zero.
    recomputed_objective: ObjectiveTuple | None
    #: Named guarantees the instance cannot support, e.g. an UNKNOWN
    #: reservation. Reported, never silently passed (§7).
    not_established: tuple[str, ...]
    #: The conservative disclosure closure the checker derived.
    recomputed_may_change: tuple[str, ...]

    @property
    def accepted(self) -> bool:
        return self.verdict == VERDICT_CHECKED_FEASIBLE

    def causes(self) -> tuple[str, ...]:
        return tuple(sorted({f.cause for f in self.findings}))


@dataclass(frozen=True)
class OptimalityVerdict:
    """The ONLY way to say "optimal" (§9.1).

    Constructed exclusively by :func:`check_optimality_artifact`, which
    RE-DERIVES the optimum and compares — it does not read the artifact's
    own number back to itself.

    :attr:`not_established` carries forward every guarantee the instance
    could not support. It is not decoration: ``certified_optimal`` is
    ``False`` whenever it is non-empty, because "optimal" over an
    objective whose reservation or cap dimension was never enforceable is
    a stronger word than the evidence carries (§7, §6 of the parent
    design)."""

    certified_optimal: bool
    method: str
    findings: tuple[CheckFinding, ...]
    #: Copied from the checked result, so the caveat cannot be lost at
    #: the point where the strongest claim in the system is made.
    not_established: tuple[str, ...] = ()

    def causes(self) -> tuple[str, ...]:
        return tuple(sorted({f.cause for f in self.findings}))


# --------------------------------------------------------------------------- #
# MayChange: the conservative disclosure closure (§9, design v8 §6).          #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class MayChangeDerivation:
    """The re-derived disclosure set and the reasons it is what it is."""

    locations: tuple[str, ...]
    #: Every action in the transitive closure the derivation reached.
    closure: tuple[str, ...]
    #: Closure members whose effect summary was UNKNOWN, forcing the
    #: whole affected scope into the set.
    widened_by: tuple[str, ...]
    #: Closure members whose revelation bound was UNKNOWN. Any entry
    #: here refuses qualified publication.
    unknown_revelation: tuple[str, ...]
    #: Obligation classes reachable from the closure that contain a
    #: ``must`` obligation. Any entry here refuses publication (§8).
    revealable_must_classes: tuple[str, ...]


def derive_may_change(
    instance: BatchInstance, deferred: Sequence[str]
) -> MayChangeDerivation:
    """Least fixed point of the MayChange rules over the deferred set.

    The closure starts at the deferred actions and grows by three rules,
    applied until nothing is added:

    1. **Prerequisites.** Every transitive prerequisite of a closure
       member joins the closure. This is deliberately taken over ALL
       transitive prerequisites, not only the unselected ones. A
       prerequisite that already ran cannot in fact change anything
       again, so this over-widens — in the fail-closed direction: the
       recomputed set is what a claimed disclosure set must CONTAIN, so
       widening can only demand more warnings, never miss a location.
    2. **Revelation.** A closure member's stated revelation names
       action classes and obligation classes it may add. Every action of
       a named action class joins the closure, and so does every action
       covering an obligation of a named obligation class — the work a
       revealed obligation would require is exactly what could change
       something.
    3. **Effects.** Each closure member contributes its stated effect
       locations. A member whose effects are UNKNOWN contributes the
       whole affected scope of the instance's changes instead (§7).

    ``L`` is finite for a frozen batch and the rules only ever add, so
    the fixed point exists and is reached in at most ``|A|`` rounds."""
    action_ids = {a.id for a in instance.actions}
    closure: set[str] = {a for a in deferred if a in action_ids}
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
            reveals = action.reveals
            if isinstance(reveals, Revelation):
                for action_class in reveals.action_classes:
                    for other in instance.actions:
                        if (other.action_class == action_class
                                and other.id not in closure):
                            closure.add(other.id)
                            changed = True
                for obligation_class in reveals.obligation_classes:
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

    whole_scope = instance.whole_affected_scope()
    locations: set[str] = set()
    widened_by: list[str] = []
    unknown_revelation: list[str] = []
    for action_id in sorted(closure):
        action = instance.action_by_id(action_id)
        if action is None:
            continue
        effects = action.effects
        if isinstance(effects, Effects):
            locations.update(effects.locations)
        else:
            widened_by.append(action_id)
            locations.update(whole_scope)
        if isinstance(action.reveals, Unknown):
            unknown_revelation.append(action_id)

    must_classes = {
        o.obligation_class for o in instance.obligations
        if o.priority == PRIORITY_MUST
    }
    revealable_must = sorted(revealed_obligation_classes & must_classes)

    return MayChangeDerivation(
        locations=tuple(sorted(locations)),
        closure=tuple(sorted(closure)),
        widened_by=tuple(widened_by),
        unknown_revelation=tuple(unknown_revelation),
        revealable_must_classes=tuple(revealable_must),
    )


# --------------------------------------------------------------------------- #
# Objective recomputation (§8).                                                #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class _Recomputation:
    objective: ObjectiveTuple | None
    findings: tuple[CheckFinding, ...]
    covered_units: frozenset[str]


def recompute_objective(
    instance: BatchInstance, plan: SchedulePlan
) -> tuple[ObjectiveTuple | None, tuple[CheckFinding, ...]]:
    """Recompute the §8 tuple from the instance. Never trusts the plan."""
    result = _recompute(instance, plan)
    return result.objective, result.findings


def _credited_units(
    instance: BatchInstance, plan: SchedulePlan
) -> tuple[frozenset[str], list[CheckFinding]]:
    """Distinct coverage units discharged by a SUCCESSFUL witness.

    Distinctness is the whole point of §6: a witness set that names the
    same unit through three cloned obligations credits it once.

    Three things must line up before a unit is credited: the witness says
    ``success``, the ACTION says ``success``, and every action in that
    action's prerequisite closure says ``success``. A witness that
    disagrees with its own action's outcome is a refutable
    contradiction, not a tie broken in the plan's favour."""
    findings: list[CheckFinding] = []
    by_id = {s.action_id: s for s in plan.selected}
    selected_ids = set(by_id)
    eligible = _succeeded_closure(instance, plan)
    credited: set[str] = set()
    for witness in plan.witnesses:
        where = (f"$.witnesses[{witness.unit_key}/{witness.obligation_id}"
                 f"/{witness.action_id}]")
        action = instance.action_by_id(witness.action_id)
        obligation = instance.obligation_by_id(witness.obligation_id)
        if action is None or obligation is None:
            findings.append(CheckFinding(
                CAUSE_WITNESS_INVALID, where,
                "witness names an action or obligation the instance does "
                "not declare"))
            continue
        if witness.action_id not in selected_ids:
            findings.append(CheckFinding(
                CAUSE_WITNESS_INVALID, where,
                "witness names an action the plan did not select"))
            continue
        if witness.obligation_id not in action.covers:
            findings.append(CheckFinding(
                CAUSE_WITNESS_INVALID, where,
                "the named action does not declare that it covers the "
                "named obligation"))
            continue
        if witness.unit_key != obligation.unit.key:
            findings.append(CheckFinding(
                CAUSE_WITNESS_INVALID, where,
                "witness unit key does not match the obligation's coverage "
                "unit; coverage credit follows the unit mapping, not the "
                "witness's own claim"))
            continue
        if witness.outcome != OUTCOME_SUCCESS:
            continue
        scheduled = by_id[witness.action_id]
        if scheduled.outcome != OUTCOME_SUCCESS:
            findings.append(CheckFinding(
                CAUSE_WITNESS_INVALID, where,
                "the witness claims success but the selected action "
                f"reports outcome {scheduled.outcome!r}; an obligation "
                "cannot be discharged by work that did not succeed",
                {"action_outcome": scheduled.outcome}))
            continue
        if witness.action_id not in eligible:
            findings.append(CheckFinding(
                CAUSE_WITNESS_INVALID, where,
                "the witnessing action stands on a prerequisite that did "
                "not succeed; coverage credit does not flow past a "
                "failure (§7)"))
            continue
        credited.add(witness.unit_key)
    return frozenset(credited), findings


def _recompute(instance: BatchInstance, plan: SchedulePlan) -> _Recomputation:
    findings: list[CheckFinding] = []
    credited, witness_findings = _credited_units(instance, plan)
    findings.extend(witness_findings)

    should_units = len(credited & set(instance.units_by_priority(
        PRIORITY_SHOULD)))
    may_units = len(credited & set(instance.units_by_priority(PRIORITY_MAY)))

    total_cost: int | None = 0
    for scheduled in sorted(plan.selected, key=lambda s: s.action_id):
        action = instance.action_by_id(scheduled.action_id)
        if action is None:
            total_cost = None
            continue
        if isinstance(action.execution_cost, Unknown):
            findings.append(CheckFinding(
                CAUSE_COST_UNKNOWN, f"$.actions[{action.id}].execution_cost",
                "operational cost is UNKNOWN, so total cost cannot be "
                "recomputed; an unmeasured action is not a free action",
                {"reason": action.execution_cost.reason}))
            total_cost = None
        elif total_cost is not None:
            total_cost += action.execution_cost

    makespan = max((s.end for s in plan.selected), default=0)
    tie_break = canonical_schedule_serialization(plan.selected, plan.deferred)

    objective: ObjectiveTuple | None = None
    if total_cost is not None:
        objective = ObjectiveTuple(
            should_units=should_units,
            may_units=may_units,
            total_cost=total_cost,
            makespan=makespan,
            tie_break=tie_break,
        )
    return _Recomputation(objective, tuple(findings), credited)


# --------------------------------------------------------------------------- #
# The check.                                                                   #
# --------------------------------------------------------------------------- #
def check_schedule(
    instance: BatchInstance,
    plan: SchedulePlan,
    certificate: ScheduleCertificate,
) -> ScheduleCheckResult:
    """Re-derive every §9 check from the canonical instance.

    Returns a typed verdict. Every refutation carries a named cause, so
    an adversarial mutation is reported as *what* it broke rather than
    as a bare failure."""
    findings: list[CheckFinding] = []
    not_established: list[str] = []

    findings.extend(_check_bindings(instance, plan, certificate))
    selected_ids, partition_findings = _check_partition(instance, plan)
    findings.extend(partition_findings)
    findings.extend(_check_acyclicity(instance))
    findings.extend(_check_durations_and_order(instance, plan, selected_ids))
    findings.extend(_check_conflicts(instance, plan, selected_ids))
    capacity_findings, capacity_not_established = _check_resources(
        instance, plan)
    findings.extend(capacity_findings)
    not_established.extend(capacity_not_established)

    recomputation = _recompute(instance, plan)
    findings.extend(recomputation.findings)
    findings.extend(_check_mandatory_coverage(instance,
                                              recomputation.covered_units))

    if recomputation.objective is None:
        findings.append(CheckFinding(
            CAUSE_OBJECTIVE_MISMATCH, "$.claimed_objective",
            "the claimed objective cannot be confirmed because a quantity "
            "it depends on is UNKNOWN"))
    elif recomputation.objective != plan.claimed_objective:
        findings.append(CheckFinding(
            CAUSE_OBJECTIVE_MISMATCH, "$.claimed_objective",
            "the claimed lexicographic tuple is not the recomputed tuple",
            {
                "claimed": canonical_json(
                    plan.claimed_objective.to_canonical_dict()),
                "recomputed": canonical_json(
                    recomputation.objective.to_canonical_dict()),
            }))

    derivation = derive_may_change(instance, plan.deferred)
    findings.extend(_check_disclosure(plan, derivation))

    verdict = VERDICT_REFUSED if findings else VERDICT_CHECKED_FEASIBLE
    return ScheduleCheckResult(
        verdict=verdict,
        findings=tuple(sorted(findings, key=lambda f: (f.cause, f.where))),
        recomputed_objective=recomputation.objective,
        not_established=tuple(sorted(not_established)),
        recomputed_may_change=derivation.locations,
    )


def _check_bindings(
    instance: BatchInstance,
    plan: SchedulePlan,
    certificate: ScheduleCertificate,
) -> list[CheckFinding]:
    findings: list[CheckFinding] = []
    for label, version in (
        ("instance", instance.schema_version),
        ("plan", plan.schema_version),
        ("certificate", certificate.schema_version),
    ):
        if version != CONTRACT_SCHEMA_VERSION:
            findings.append(CheckFinding(
                CAUSE_SCHEMA_VERSION_MISMATCH, f"$.{label}.schema_version",
                f"expected {CONTRACT_SCHEMA_VERSION!r}, got {version!r}"))
    if plan.instance_id != instance.instance_id:
        findings.append(CheckFinding(
            CAUSE_INSTANCE_DIGEST_MISMATCH, "$.plan.instance_id",
            "the plan names a different instance"))
    if certificate.instance_digest != instance.digest:
        findings.append(CheckFinding(
            CAUSE_INSTANCE_DIGEST_MISMATCH, "$.certificate.instance_digest",
            "the certificate does not bind this instance",
            {"expected": instance.digest,
             "claimed": certificate.instance_digest}))
    if certificate.unit_mapping_digest != instance.unit_mapping_digest:
        findings.append(CheckFinding(
            CAUSE_UNIT_MAPPING_DIGEST_MISMATCH,
            "$.certificate.unit_mapping_digest",
            "the certificate does not bind this obligation-to-unit mapping",
            {"expected": instance.unit_mapping_digest,
             "claimed": certificate.unit_mapping_digest}))
    if certificate.plan_digest != plan.digest:
        findings.append(CheckFinding(
            CAUSE_PLAN_DIGEST_MISMATCH, "$.certificate.plan_digest",
            "the certificate does not bind this plan",
            {"expected": plan.digest, "claimed": certificate.plan_digest}))
    if certificate.claimed_objective != plan.claimed_objective:
        findings.append(CheckFinding(
            CAUSE_CERTIFICATE_OBJECTIVE_MISMATCH,
            "$.certificate.claimed_objective",
            "the certificate and the plan claim different objective tuples"))
    if certificate.checker_version != CHECKER_VERSION:
        findings.append(CheckFinding(
            CAUSE_SCHEMA_VERSION_MISMATCH, "$.certificate.checker_version",
            f"certificate names checker {certificate.checker_version!r}, "
            f"this checker is {CHECKER_VERSION!r}"))
    findings.extend(_check_producer_binding(certificate))
    return findings


def _is_digest(value: str) -> bool:
    return (len(value) == 64
            and all(c in "0123456789abcdef" for c in value))


def _check_producer_binding(
    certificate: ScheduleCertificate,
) -> list[CheckFinding]:
    """§12's producer and artifact bindings are well-formed.

    Well-formed, not believed: this establishes that the certificate SAYS
    who produced the plan and which artifact it stands behind. Whether
    the artifact supports its claim is :func:`check_optimality_artifact`'s
    question."""
    findings: list[CheckFinding] = []
    producer = certificate.producer
    where = "$.certificate.producer"
    if producer.termination_status not in TERMINATION_STATUSES:
        findings.append(CheckFinding(
            CAUSE_PRODUCER_BINDING_INVALID, f"{where}.termination_status",
            f"termination status {producer.termination_status!r} is not one "
            f"of {TERMINATION_STATUSES}"))
    for label, value in (("family", producer.family),
                         ("version", producer.version),
                         ("parameters_digest", producer.parameters_digest)):
        if not value:
            findings.append(CheckFinding(
                CAUSE_PRODUCER_BINDING_INVALID, f"{where}.{label}",
                "a certificate that does not name its producer binds "
                "nothing to a reviewable identity (§12)"))
    bound = certificate.optimality_artifact_digest
    if bound != ARTIFACT_UNBOUND and not _is_digest(bound):
        findings.append(CheckFinding(
            CAUSE_PRODUCER_BINDING_INVALID,
            "$.certificate.optimality_artifact_digest",
            f"expected {ARTIFACT_UNBOUND!r} or a sha256 digest, got "
            f"{bound!r}"))
    return findings


def _check_partition(
    instance: BatchInstance, plan: SchedulePlan
) -> tuple[set[str], list[CheckFinding]]:
    """Selected and deferred must partition the instance's actions."""
    findings: list[CheckFinding] = []
    action_ids = {a.id for a in instance.actions}
    selected_ids = {s.action_id for s in plan.selected}
    deferred_ids = set(plan.deferred)

    for action_id in sorted(selected_ids | deferred_ids):
        if action_id not in action_ids:
            findings.append(CheckFinding(
                CAUSE_IDENTITY_PARTITION, f"$.plan[{action_id}]",
                "the plan names an action the instance does not declare"))
    overlap = sorted(selected_ids & deferred_ids)
    for action_id in overlap:
        findings.append(CheckFinding(
            CAUSE_IDENTITY_PARTITION, f"$.plan[{action_id}]",
            "an action is both selected and deferred"))
    for action_id in sorted(action_ids - selected_ids - deferred_ids):
        findings.append(CheckFinding(
            CAUSE_IDENTITY_PARTITION, f"$.plan[{action_id}]",
            "the plan neither selects nor defers a declared action; "
            "silence is not a decision"))
    return selected_ids, findings


def _check_acyclicity(instance: BatchInstance) -> list[CheckFinding]:
    """Kahn's algorithm over ``requires``. A cycle is unschedulable."""
    remaining = {a.id: set(a.requires) for a in instance.actions}
    progress = True
    while progress:
        progress = False
        for action_id in sorted(remaining):
            if not remaining[action_id]:
                del remaining[action_id]
                for deps in remaining.values():
                    deps.discard(action_id)
                progress = True
                break
    if remaining:
        return [CheckFinding(
            CAUSE_DEPENDENCY_CYCLE, "$.actions",
            "the prerequisite graph contains a cycle",
            {"actions": ",".join(sorted(remaining))})]
    return []


def _check_durations_and_order(
    instance: BatchInstance, plan: SchedulePlan, selected_ids: set[str]
) -> list[CheckFinding]:
    findings: list[CheckFinding] = []
    by_id = {s.action_id: s for s in plan.selected}
    for scheduled in sorted(plan.selected, key=lambda s: s.action_id):
        action = instance.action_by_id(scheduled.action_id)
        if action is None:
            continue
        where = f"$.selected[{scheduled.action_id}]"
        if isinstance(action.duration, Unknown):
            findings.append(CheckFinding(
                CAUSE_DURATION_UNKNOWN, where,
                "duration is UNKNOWN, so neither the interval nor any "
                "capacity event that depends on it can be established",
                {"reason": action.duration.reason}))
        elif scheduled.end - scheduled.start != action.duration:
            findings.append(CheckFinding(
                CAUSE_DURATION_MISMATCH, where,
                "the scheduled interval does not equal the pinned "
                "duration; actions are non-preemptive (§7)",
                {"duration": action.duration,
                 "interval": scheduled.end - scheduled.start}))
        for prerequisite in action.requires:
            if prerequisite not in selected_ids:
                findings.append(CheckFinding(
                    CAUSE_PREREQUISITE_NOT_SELECTED, f"{where}.requires",
                    f"prerequisite {prerequisite!r} is not selected; "
                    "prerequisite closure is mandatory (§7)"))
                continue
            earlier = by_id[prerequisite]
            if scheduled.start < earlier.end:
                findings.append(CheckFinding(
                    CAUSE_TEMPORAL_ORDER_VIOLATION, f"{where}.requires",
                    f"starts at {scheduled.start} before prerequisite "
                    f"{prerequisite!r} ends at {earlier.end}"))
            # §7's third conjunct: `a` may start only after `b`
            # SUCCEEDS. Selected-and-earlier is not success, and a
            # prerequisite whose outcome is `failure` or `unknown`
            # licenses nothing downstream of it.
            if earlier.outcome != OUTCOME_SUCCESS:
                findings.append(CheckFinding(
                    CAUSE_PREREQUISITE_NOT_SUCCEEDED, f"{where}.requires",
                    f"prerequisite {prerequisite!r} reports outcome "
                    f"{earlier.outcome!r}; §7 admits `a` only after `b` "
                    "SUCCEEDS, and an unknown outcome is an admission, "
                    "not a pass",
                    {"prerequisite": prerequisite,
                     "outcome": earlier.outcome}))
    return findings


def _succeeded_closure(
    instance: BatchInstance, plan: SchedulePlan
) -> frozenset[str]:
    """Selected actions that succeeded AND whose whole prerequisite
    closure succeeded.

    Coverage credit is a claim that work was DONE. An action standing on
    a failed prerequisite did not do it, whatever its own witness says,
    so credit stops at the first failure in the chain rather than
    flowing past it."""
    by_id = {s.action_id: s for s in plan.selected}
    eligible = {
        action_id for action_id, scheduled in by_id.items()
        if scheduled.outcome == OUTCOME_SUCCESS
    }
    changed = True
    while changed:
        changed = False
        for action_id in sorted(eligible):
            action = instance.action_by_id(action_id)
            if action is None:
                continue
            for prerequisite in action.requires:
                if prerequisite not in eligible:
                    eligible.discard(action_id)
                    changed = True
                    break
    return frozenset(eligible)


def _running_at(
    plan: SchedulePlan, time: int
) -> list[ScheduledAction]:
    return [s for s in sorted(plan.selected, key=lambda s: s.action_id)
            if s.start <= time < s.end]


def _event_times(instance: BatchInstance, plan: SchedulePlan) -> list[int]:
    """Every action start, every action end, and every breakpoint (§7).

    Including the breakpoints is what catches a capacity that drops in
    the MIDDLE of a running action — a violation no start-time-only
    check can see."""
    events: set[int] = set()
    for scheduled in plan.selected:
        events.add(scheduled.start)
        events.add(scheduled.end)
    for resource in instance.resources:
        for segment in resource.segments:
            events.add(segment.start)
    return sorted(events)


def _check_conflicts(
    instance: BatchInstance, plan: SchedulePlan, selected_ids: set[str]
) -> list[CheckFinding]:
    findings: list[CheckFinding] = []
    by_id = {s.action_id: s for s in plan.selected}

    # A pairwise conflict is symmetric even when only one side declares
    # it, so normalize to unordered pairs before testing overlap. Reading
    # only the lower-id side's declaration would miss a conflict spelled
    # exclusively on the higher-id action.
    pairs: set[tuple[str, str]] = set()
    for action in instance.actions:
        for other_id in action.conflicts:
            if action.id in selected_ids and other_id in selected_ids:
                pairs.add(tuple(sorted((action.id, other_id))))  # type: ignore[arg-type]
    for left, right in sorted(pairs):
        first, second = by_id[left], by_id[right]
        if first.start < second.end and second.start < first.end:
            findings.append(CheckFinding(
                CAUSE_CONFLICT_VIOLATION, f"$.conflicts[{left},{right}]",
                "conflicting actions overlap in time"))

    for group in sorted(instance.conflict_groups, key=lambda g: g.id):
        members = set(group.members) & selected_ids
        for time in _event_times(instance, plan):
            running = [s.action_id for s in _running_at(plan, time)
                       if s.action_id in members]
            if len(running) > group.max_concurrent:
                findings.append(CheckFinding(
                    CAUSE_CONFLICT_GROUP_VIOLATION,
                    f"$.conflict_groups[{group.id}]@{time}",
                    f"{len(running)} members run concurrently but the "
                    f"group admits {group.max_concurrent}",
                    {"running": ",".join(sorted(running))}))
                break
    return findings


def _declared_demand(action: Action, resource_id: str) -> int | Unknown:
    """An omitted demand key is UNKNOWN, never zero.

    §7 declares ``demand(a, r)`` for EACH named resource, and the loader
    refuses an instance that leaves one out. This function is the second
    lock on the same door: a hand-built :class:`BatchInstance` that never
    went through the loader still cannot buy a permissive zero by staying
    silent. Zero demand is expressible — by writing ``0``."""
    if resource_id not in action.demand:
        return Unknown(
            f"the action's demand mapping is silent about resource "
            f"{resource_id!r}; an absent key is not a zero")
    return action.demand[resource_id]


def _check_resources(
    instance: BatchInstance, plan: SchedulePlan
) -> tuple[list[CheckFinding], list[str]]:
    findings: list[CheckFinding] = []
    not_established: list[str] = []
    events = _event_times(instance, plan)

    # Assignments must equal declared demand before any usage sum means
    # anything.
    for scheduled in sorted(plan.selected, key=lambda s: s.action_id):
        action = instance.action_by_id(scheduled.action_id)
        if action is None:
            continue
        where = f"$.selected[{scheduled.action_id}].resource_assignments"
        for resource in instance.resources:
            demand = _declared_demand(action, resource.id)
            assigned = scheduled.resource_assignments.get(resource.id, 0)
            if isinstance(demand, Unknown):
                findings.append(CheckFinding(
                    CAUSE_DEMAND_UNKNOWN, f"{where}.{resource.id}",
                    "demand is UNKNOWN, so no capacity claim about this "
                    "resource can be established",
                    {"reason": demand.reason}))
            elif assigned != demand:
                findings.append(CheckFinding(
                    CAUSE_RESOURCE_ASSIGNMENT_MISMATCH,
                    f"{where}.{resource.id}",
                    f"assigns {assigned} but the action declares demand "
                    f"{demand}"))
        for resource_id in sorted(scheduled.resource_assignments):
            if instance.resource_by_id(resource_id) is None:
                findings.append(CheckFinding(
                    CAUSE_RESOURCE_ASSIGNMENT_MISMATCH,
                    f"{where}.{resource_id}",
                    "assignment names a resource the instance does not "
                    "declare"))

    for resource in sorted(instance.resources, key=lambda r: r.id):
        reported_exceeded = False
        reported_unknown = False
        for time in events:
            usage = 0
            for scheduled in _running_at(plan, time):
                usage += scheduled.resource_assignments.get(resource.id, 0)
            if usage == 0:
                continue
            capacity = resource.capacity_at(time)
            if isinstance(capacity, Unknown):
                if not reported_unknown:
                    findings.append(CheckFinding(
                        CAUSE_CAPACITY_UNKNOWN,
                        f"$.resources[{resource.id}]@{time}",
                        "capacity in force at a schedule event is UNKNOWN "
                        "while the resource is in use",
                        {"reason": capacity.reason}))
                    reported_unknown = True
            elif usage > capacity and not reported_exceeded:
                findings.append(CheckFinding(
                    CAUSE_CAPACITY_EXCEEDED,
                    f"$.resources[{resource.id}]@{time}",
                    f"usage {usage} exceeds capacity {capacity} at a "
                    "schedule event",
                    {"time": time, "usage": usage, "capacity": capacity}))
                reported_exceeded = True
            if isinstance(resource.hard_cap, int) and usage > resource.hard_cap:
                findings.append(CheckFinding(
                    CAUSE_HARD_CAP_EXCEEDED,
                    f"$.resources[{resource.id}]@{time}",
                    f"usage {usage} exceeds the enforceable hard cap "
                    f"{resource.hard_cap}"))
                break
        if isinstance(resource.hard_cap, Unknown):
            not_established.append(f"hard_cap:{resource.id}")

        consumed: int | None = 0
        for scheduled in plan.selected:
            action = instance.action_by_id(scheduled.action_id)
            if action is None:
                continue
            demand = _declared_demand(action, resource.id)
            if isinstance(demand, Unknown) or isinstance(action.duration,
                                                         Unknown):
                consumed = None
                break
            if consumed is not None:
                consumed += demand * action.duration
        if isinstance(resource.reservation, Unknown):
            not_established.append(f"reservation:{resource.id}")
        elif consumed is None:
            not_established.append(f"reservation:{resource.id}")
        elif consumed > resource.reservation:
            findings.append(CheckFinding(
                CAUSE_RESERVATION_EXCEEDED, f"$.resources[{resource.id}]",
                f"the plan consumes {consumed} against an enforceable "
                f"reservation of {resource.reservation}",
                {"consumed": consumed, "reservation": resource.reservation}))
    return findings, not_established


def _check_mandatory_coverage(
    instance: BatchInstance, credited: frozenset[str]
) -> list[CheckFinding]:
    """§8's hard feasibility condition: zero unresolved ``must``.

    Not a penalty term. No amount of ``should`` coverage compensates."""
    findings: list[CheckFinding] = []
    for unit_key in instance.units_by_priority(PRIORITY_MUST):
        if unit_key not in credited:
            findings.append(CheckFinding(
                CAUSE_MANDATORY_UNIT_UNCOVERED, "$.coverage",
                "a known mandatory coverage unit has no successful "
                "witness; publication is refused",
                {"unit_key": unit_key}))
    return findings


def _check_disclosure(
    plan: SchedulePlan, derivation: MayChangeDerivation
) -> list[CheckFinding]:
    findings: list[CheckFinding] = []
    claimed = set(plan.may_change)
    missing = sorted(set(derivation.locations) - claimed)
    if missing:
        findings.append(CheckFinding(
            CAUSE_MAY_CHANGE_INCOMPLETE, "$.may_change",
            "the claimed disclosure set omits locations the deferred "
            "closure reaches",
            {"missing": ",".join(missing)}))
    for action_id in derivation.unknown_revelation:
        findings.append(CheckFinding(
            CAUSE_UNKNOWN_REVELATION, f"$.actions[{action_id}].reveals",
            "a deferred-closure action has an UNKNOWN revelation bound; "
            "it is treated as able to reveal every applicable configured "
            "`must`, and qualified publication is refused (§9)"))
    for obligation_class in derivation.revealable_must_classes:
        findings.append(CheckFinding(
            CAUSE_DEFERRED_MAY_REVEAL_MUST, "$.deferred",
            "deferred work may reveal a mandatory obligation class; §8's "
            "feasibility condition fails",
            {"obligation_class": obligation_class}))
    return findings


# --------------------------------------------------------------------------- #
# The checker's OWN re-derivation of the optimum (§9.1).                       #
# --------------------------------------------------------------------------- #
# Why this exists: an artifact whose only test is "your best value equals
# your own recomputed value" tests nothing an adversary cannot supply.
# §9.1 is explicit that "a solver status string, feasibility certificate,
# or recomputed objective value is not an optimality proof". The only way
# to check exhaustive enumeration is to redo it, so the checker redoes
# it, within a bound it declares for itself and refuses beyond.
#
# The enumerated class is the ACTIVE schedules over the instance's
# canonical event lattice, characterized in the trusted base by
# `enumeration_horizon` and `enumeration_breakpoints`: start at time 0,
# place any ready action at the current time, or advance to the next
# action end or capacity breakpoint. The lexicographic optimum is itself
# active — stages 1-3 depend only on the selected set, stage 4 is
# regular, and stage 5 sorts `end` before `start`, so a legal left-shift
# never worsens the tuple — so enumerating active schedules suffices.
#
# Candidate construction is canonical and all-success on purpose: for a
# fixed placement, marking any action `failure` or `unknown` can only
# remove coverage credit, never add it, and leaves cost, makespan, and
# the (outcome-free) tie-break untouched. The all-success candidate
# therefore weakly dominates every outcome variant of the same placement,
# and enumerating outcomes would only re-derive the same optimum.
class _RecheckBudgetExceeded(Exception):
    """The checker's own enumeration bound was hit. Refuse, never cap."""


@dataclass(frozen=True)
class _Recheck:
    optimum: ObjectiveTuple | None
    candidates: int


def _enumerate_active_placements(
    instance: BatchInstance, *, node_budget: int
) -> list[tuple[tuple[str, int], ...]]:
    durations: dict[str, int] = {}
    for action in instance.actions:
        assert isinstance(action.duration, int)
        durations[action.id] = action.duration
    requires = {a.id: tuple(a.requires) for a in instance.actions}
    horizon = enumeration_horizon(instance)
    breakpoints = set(enumeration_breakpoints(instance))

    found: set[tuple[tuple[str, int], ...]] = set()
    seen: set[tuple[int, tuple[tuple[str, int], ...]]] = set()
    budget = [node_budget]

    def expand(time: int, placements: tuple[tuple[str, int], ...]) -> None:
        state = (time, placements)
        if state in seen:
            return
        seen.add(state)
        budget[0] -= 1
        if budget[0] < 0:
            raise _RecheckBudgetExceeded()
        found.add(placements)
        ends = {
            action_id: start + durations[action_id]
            for action_id, start in placements
        }
        for action_id in sorted(durations):
            if action_id in ends:
                continue
            if any(prereq not in ends or ends[prereq] > time
                   for prereq in requires[action_id]):
                continue
            if time + durations[action_id] > horizon:
                continue
            expand(time, tuple(sorted(placements + ((action_id, time),))))
        later = sorted(
            t for t in set(ends.values()) | breakpoints
            if time < t <= horizon
        )
        if later:
            expand(later[0], placements)

    expand(0, ())
    return sorted(found)


def _candidate_plan(
    instance: BatchInstance, placements: tuple[tuple[str, int], ...]
) -> SchedulePlan | None:
    """The canonical all-success candidate for one placement set."""
    placed = dict(placements)
    selected: list[ScheduledAction] = []
    witnesses: list[CoverageWitness] = []
    for action_id, start in sorted(placements):
        action = instance.action_by_id(action_id)
        if action is None or not isinstance(action.duration, int):
            return None
        assignments: dict[str, int] = {}
        for resource in instance.resources:
            demand = _declared_demand(action, resource.id)
            if isinstance(demand, Unknown):
                return None
            if demand:
                assignments[resource.id] = demand
        selected.append(ScheduledAction(
            action_id=action_id,
            start=start,
            end=start + action.duration,
            resource_assignments=assignments,
            outcome=OUTCOME_SUCCESS,
        ))
        for obligation_id in action.covers:
            obligation = instance.obligation_by_id(obligation_id)
            if obligation is None:
                return None
            witnesses.append(CoverageWitness(
                unit_key=obligation.unit.key,
                obligation_id=obligation_id,
                action_id=action_id,
                outcome=OUTCOME_SUCCESS,
            ))
    deferred = tuple(sorted(
        a.id for a in instance.actions if a.id not in placed))
    plan = SchedulePlan(
        schema_version=CONTRACT_SCHEMA_VERSION,
        instance_id=instance.instance_id,
        selected=tuple(selected),
        deferred=deferred,
        witnesses=tuple(witnesses),
        may_change=derive_may_change(instance, deferred).locations,
        claimed_objective=ObjectiveTuple(0, 0, 0, 0, ""),
    )
    recomputation = _recompute(instance, plan)
    if recomputation.objective is None:
        return None
    return replace(plan, claimed_objective=recomputation.objective)


def _self_certificate(
    instance: BatchInstance, plan: SchedulePlan
) -> ScheduleCertificate:
    """A binding for a candidate the checker built for its own search."""
    return ScheduleCertificate(
        schema_version=CONTRACT_SCHEMA_VERSION,
        instance_digest=instance.digest,
        unit_mapping_digest=instance.unit_mapping_digest,
        plan_digest=plan.digest,
        claimed_objective=plan.claimed_objective,
        checker_version=CHECKER_VERSION,
        producer=ProducerRef(
            family="batch-schedule-checker-recheck",
            version=CHECKER_VERSION,
            parameters_digest=object_digest(
                {"node_budget": OPTIMALITY_RECHECK_NODE_BUDGET,
                 "max_actions": OPTIMALITY_RECHECK_MAX_ACTIONS}),
            seed=Unknown("the re-derivation is deterministic and unseeded"),
            termination_status=TERMINATION_STOPPED,
        ),
        optimality_artifact_digest=ARTIFACT_UNBOUND,
    )


def _recompute_optimum(
    instance: BatchInstance, *, node_budget: int
) -> _Recheck:
    """Re-derive the §8 optimum from the instance alone.

    Every enumerated candidate is judged by this module's own
    :func:`check_schedule`, so the optimum is the best value over
    candidates the CHECKER accepts, not over candidates a producer
    proposed."""
    placement_sets = _enumerate_active_placements(
        instance, node_budget=node_budget)
    best_key: tuple[Any, ...] | None = None
    best: ObjectiveTuple | None = None
    for placements in placement_sets:
        plan = _candidate_plan(instance, placements)
        if plan is None:
            continue
        outcome = check_schedule(instance, plan,
                                 _self_certificate(instance, plan))
        if not outcome.accepted or outcome.recomputed_objective is None:
            continue
        key = objective_sort_key(outcome.recomputed_objective)
        if best_key is None or key < best_key:
            best_key, best = key, outcome.recomputed_objective
    return _Recheck(optimum=best, candidates=len(placement_sets))


# --------------------------------------------------------------------------- #
# The optimality boundary (§9.1).                                              #
# --------------------------------------------------------------------------- #
def check_optimality_artifact(
    instance: BatchInstance,
    result: ScheduleCheckResult,
    artifact: OptimalityArtifact,
    *,
    certificate: ScheduleCertificate,
) -> OptimalityVerdict:
    """The only route to the word "optimal", and it VERIFIES, not echoes.

    The artifact must bind this instance and this unit mapping; the
    certificate must bind this artifact and this candidate; and — the
    part that matters — the checker re-derives the optimum itself and
    requires the artifact's best tuple to equal BOTH the recomputed
    optimum AND the candidate's checked value, while the reported search
    record (candidate count, search-space digest, declared bounds,
    producer version) must match the recomputation. A hand-written
    artifact agreeing with its own candidate is therefore not a proof of
    anything, which is §9.1's whole point.

    Beyond the checker's declared re-derivation bound
    (:data:`OPTIMALITY_RECHECK_MAX_ACTIONS`,
    :data:`OPTIMALITY_RECHECK_NODE_BUDGET`), and for
    ``checked_bound`` — for which no checked proof format exists yet —
    the claim is REFUSED rather than granted. §9.1's fallback stands:
    such results are reported as checked feasible candidates with checked
    values and empirical gaps only."""
    findings: list[CheckFinding] = []
    verifiable = True

    if artifact.method not in OPTIMALITY_METHODS:
        findings.append(CheckFinding(
            CAUSE_ARTIFACT_METHOD_UNKNOWN, "$.artifact.method",
            f"method {artifact.method!r} is not one of "
            f"{OPTIMALITY_METHODS}; a solver status string is not a proof"))
        verifiable = False
    elif artifact.method != OPTIMALITY_METHOD_EXHAUSTIVE:
        findings.append(CheckFinding(
            CAUSE_ARTIFACT_SEARCH_UNVERIFIABLE, "$.artifact.method",
            f"method {artifact.method!r} has no checked proof format in "
            "this slice: there is nothing here that can verify a "
            "solver-independent bound, and a bound this checker cannot "
            "redo is an assertion (§9.1)"))
        verifiable = False

    if not result.accepted or result.recomputed_objective is None:
        findings.append(CheckFinding(
            CAUSE_ARTIFACT_REQUIRES_FEASIBLE_CHECK, "$.result",
            "optimality is only expressible over a candidate this checker "
            "found feasible with a recomputed value"))
    if artifact.instance_digest != instance.digest:
        findings.append(CheckFinding(
            CAUSE_ARTIFACT_DIGEST_MISMATCH, "$.artifact.instance_digest",
            "the artifact was produced for a different instance"))
        verifiable = False
    if artifact.unit_mapping_digest != instance.unit_mapping_digest:
        findings.append(CheckFinding(
            CAUSE_ARTIFACT_DIGEST_MISMATCH, "$.artifact.unit_mapping_digest",
            "the artifact was produced for a different unit mapping"))
        verifiable = False

    findings.extend(_check_artifact_binding(instance, result, artifact,
                                            certificate))

    if artifact.producer_version not in REVIEWED_OPTIMALITY_PRODUCERS:
        findings.append(CheckFinding(
            CAUSE_ARTIFACT_PRODUCER_UNREVIEWED,
            "$.artifact.producer_version",
            f"producer {artifact.producer_version!r} is not one of the "
            f"reviewed producers {REVIEWED_OPTIMALITY_PRODUCERS}"))

    for label in result.not_established:
        findings.append(CheckFinding(
            CAUSE_ARTIFACT_UNESTABLISHED_BOUND, "$.result.not_established",
            "the checked result could not establish a bound the objective "
            "depends on; 'optimal' is not available over an objective with "
            "an unenforceable dimension (§7)",
            {"guarantee": label}))

    if (result.recomputed_objective is not None
            and artifact.best_objective != result.recomputed_objective):
        findings.append(CheckFinding(
            CAUSE_ARTIFACT_VALUE_MISMATCH, "$.artifact.best_objective",
            "the artifact's best value differs from the candidate's "
            "checked value at some objective stage",
            {
                "artifact": canonical_json(
                    artifact.best_objective.to_canonical_dict()),
                "checked": canonical_json(
                    result.recomputed_objective.to_canonical_dict()),
            }))

    if verifiable:
        findings.extend(_recheck_optimum(instance, result, artifact))

    return OptimalityVerdict(
        certified_optimal=not findings,
        method=artifact.method,
        findings=tuple(sorted(findings, key=lambda f: (f.cause, f.where))),
        not_established=result.not_established,
    )


def _check_artifact_binding(
    instance: BatchInstance,
    result: ScheduleCheckResult,
    artifact: OptimalityArtifact,
    certificate: ScheduleCertificate,
) -> list[CheckFinding]:
    """§12: an optimality claim names the artifact it stands behind.

    Without this, any artifact could be paired with any certificate over
    the same instance, which is the structural half of the forged-artifact
    problem: the objects that must agree were never tied together."""
    findings: list[CheckFinding] = []
    where = "$.certificate.optimality_artifact_digest"
    if certificate.optimality_artifact_digest == ARTIFACT_UNBOUND:
        findings.append(CheckFinding(
            CAUSE_ARTIFACT_NOT_BOUND, where,
            "the certificate binds no optimality artifact, so it cannot "
            "carry an optimality claim (§12)"))
    elif certificate.optimality_artifact_digest != artifact.digest:
        findings.append(CheckFinding(
            CAUSE_ARTIFACT_NOT_BOUND, where,
            "the certificate binds a different optimality artifact",
            {"expected": artifact.digest,
             "bound": certificate.optimality_artifact_digest}))
    if certificate.instance_digest != instance.digest:
        findings.append(CheckFinding(
            CAUSE_ARTIFACT_NOT_BOUND, "$.certificate.instance_digest",
            "the certificate was produced for a different instance"))
    if (result.recomputed_objective is not None
            and certificate.claimed_objective != result.recomputed_objective):
        findings.append(CheckFinding(
            CAUSE_ARTIFACT_NOT_BOUND, "$.certificate.claimed_objective",
            "the certificate does not stand behind the checked candidate"))
    if certificate.producer.termination_status != TERMINATION_EXHAUSTED:
        findings.append(CheckFinding(
            CAUSE_ARTIFACT_SEARCH_UNVERIFIABLE,
            "$.certificate.producer.termination_status",
            f"the producer reports {certificate.producer.termination_status!r};"
            " an exhaustive claim requires a search that finished"))
    return findings


def _recheck_optimum(
    instance: BatchInstance,
    result: ScheduleCheckResult,
    artifact: OptimalityArtifact,
) -> list[CheckFinding]:
    findings: list[CheckFinding] = []
    where = "$.artifact.search_space_digest"
    if len(instance.actions) > OPTIMALITY_RECHECK_MAX_ACTIONS:
        return [CheckFinding(
            CAUSE_ARTIFACT_SEARCH_UNVERIFIABLE, "$.instance.actions",
            f"the instance declares {len(instance.actions)} actions and this "
            f"checker re-derives an optimum for at most "
            f"{OPTIMALITY_RECHECK_MAX_ACTIONS}; beyond that there is no "
            "checked proof format, so the claim is refused (§9.1)")]
    if artifact.max_actions > OPTIMALITY_RECHECK_MAX_ACTIONS:
        findings.append(CheckFinding(
            CAUSE_ARTIFACT_SEARCH_UNVERIFIABLE, "$.artifact.max_actions",
            f"the artifact declares a bound of {artifact.max_actions}; the "
            "checker's declared bound is "
            f"{OPTIMALITY_RECHECK_MAX_ACTIONS} and an artifact does not "
            "extend it"))
    if artifact.node_budget > OPTIMALITY_RECHECK_NODE_BUDGET:
        findings.append(CheckFinding(
            CAUSE_ARTIFACT_SEARCH_UNVERIFIABLE, "$.artifact.node_budget",
            f"the artifact declares a node budget of {artifact.node_budget}; "
            f"the checker will redo at most "
            f"{OPTIMALITY_RECHECK_NODE_BUDGET} nodes"))
    if findings:
        return findings

    try:
        recheck = _recompute_optimum(
            instance, node_budget=OPTIMALITY_RECHECK_NODE_BUDGET)
    except _RecheckBudgetExceeded:
        return [CheckFinding(
            CAUSE_ARTIFACT_SEARCH_UNVERIFIABLE, where,
            "the checker's own re-derivation exceeded its declared node "
            "budget; it refuses rather than certifying a search it did not "
            "finish")]
    except BatchLoadRefused as refusal:
        return [CheckFinding(
            CAUSE_ARTIFACT_SEARCH_UNVERIFIABLE, where,
            "the search space is not characterizable on this instance",
            {"detail": str(refusal)})]

    if artifact.candidates_examined != recheck.candidates:
        findings.append(CheckFinding(
            CAUSE_ARTIFACT_SEARCH_MISMATCH, "$.artifact.candidates_examined",
            f"the artifact reports {artifact.candidates_examined} enumerated "
            f"candidates; the checker's own enumeration of the same declared "
            f"space found {recheck.candidates}",
            {"reported": artifact.candidates_examined,
             "recomputed": recheck.candidates}))
    expected_digest = search_space_digest(
        instance,
        method=artifact.method,
        max_actions=artifact.max_actions,
        node_budget=artifact.node_budget,
        candidates_examined=recheck.candidates,
    )
    if artifact.search_space_digest != expected_digest:
        findings.append(CheckFinding(
            CAUSE_ARTIFACT_SEARCH_MISMATCH, where,
            "the artifact's search-space digest is not the digest of the "
            "space the checker recomputed",
            {"expected": expected_digest,
             "claimed": artifact.search_space_digest}))
    if recheck.optimum is None:
        findings.append(CheckFinding(
            CAUSE_ARTIFACT_SEARCH_MISMATCH, where,
            "the checker's re-derivation found no feasible candidate at "
            "all, so there is no optimum for this artifact to report"))
        return findings
    if artifact.best_objective != recheck.optimum:
        findings.append(CheckFinding(
            CAUSE_ARTIFACT_VALUE_MISMATCH, "$.artifact.best_objective",
            "the artifact's best value is not the optimum this checker "
            "re-derived from the instance",
            {"artifact": canonical_json(
                artifact.best_objective.to_canonical_dict()),
             "recomputed": canonical_json(
                 recheck.optimum.to_canonical_dict())}))
    if (result.recomputed_objective is not None
            and result.recomputed_objective != recheck.optimum):
        findings.append(CheckFinding(
            CAUSE_ARTIFACT_CANDIDATE_NOT_OPTIMUM, "$.result",
            "the candidate's checked value is not the re-derived optimum; a "
            "suboptimal plan cannot be certified optimal by any artifact",
            {"checked": canonical_json(
                result.recomputed_objective.to_canonical_dict()),
             "optimum": canonical_json(recheck.optimum.to_canonical_dict())}))
    return findings
