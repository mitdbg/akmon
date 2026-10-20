"""E5 adaptive replanning: epoch invalidation, admission stop, disclosure.

PACT ``concord-disclosure-budget``, task.batch.adaptive-replanning.
Normative sections: ``designs/batch-scheduling-research.md`` §7 (the epoch
closure rule), §9 (the unknown-revelation refusal), §11 preamble
(preregistration), §11 E5 (the reveal scripts, the policies, the metrics,
and the falsification clauses), and §12 (reproducibility). The E4 lane
(:mod:`okg.bench.synth.batch_resource_replay`) and the E2 lane
(:mod:`okg.bench.synth.batch_experiments`) are the reference idioms for
the preregistered manifest with a pinned digest, refusal-aware
denominators, derived verdicts with a partial vocabulary, named
deferrals, and secondary metrics. This module follows them and adds one
thing they do not have: an ENFORCED epoch.

What this module is
-------------------
An experiment harness for §7's last paragraph:

    An execution result may reveal obligations or invalidate an estimate.
    That closes the epoch: stop admitting not-yet-started actions from the
    old plan, carry already-running actions into the next instance as
    fixed reservations, bind their eventual observations, and replan the
    remaining batch. G5 never crosses epochs.

The frozen contract CARRIES ``BatchInstance.epoch`` and digests it, and
that is all it does — the checker has no epoch cause and no epoch
guarantee, and :mod:`.batch_schedule_checker` correctly claims none. The
enforcement is therefore built HERE, in the experiment layer, and it is
built as a real refusal rather than as a report field: every admission in
this module goes through :class:`AdmissionLedger`, which raises
:class:`StalePlanAdmission` when a plan whose digest has gone stale tries
to admit an action that had not started. A policy cannot opt out of it,
because there is no other door.

Nothing here claims the CONTRACT enforces an epoch. It does not, and
:data:`DEFERRED` says so by name.

:func:`~...batch_schedule_checker.check_schedule` remains the only
validity authority. This module never decides whether a plan is feasible,
never re-derives an objective, and never softens a refusal. Where a
judgement about a plan is needed — the fixed-reservation proof is the
whole of that category — the judgement is obtained by handing the checker
an instance and a plan and reading its verdict.

The word "optimal" does not appear in this module's vocabulary and no
oracle arm runs: §11 E5's metric list names none, §9.1 permits the word
only through a checked artifact, and §7 says G5 never crosses epochs —
which is exactly what a multi-epoch experiment does.

Disclosure ownership: what was searched, what is consumed, what is deferred
--------------------------------------------------------------------------
Design §3 assigns "reader-visible identity of trimmed or unevaluated
work" to the PACT ``consistency-budget-and-disclosure`` and instructs:
"Reuse its disclosure surface. Do not add a second ``MayChange``
publication channel." That surface was located before any disclosure code
here was written, and the search is recorded in
:data:`DISCLOSURE_OWNERSHIP` so a reader can audit the conclusion rather
than take it. The finding splits in two:

* **The identity VOCABULARY is reusable, and this module consumes it.**
  :func:`~...observations.scope_identity`,
  :func:`~...observations.scope_digest`, and
  :data:`~...observations.SCOPE_DIGEST_ALGORITHM` are id-agnostic — the
  digest is defined over ``sorted(str(v) for v in ids)`` — so an action
  id, a coverage-unit key, and a result-location string are all
  well-formed members. Every disclosure row this module emits is built by
  calling ``scope_identity`` directly. There is no second digest
  algorithm, no second truncation rule, and no second identity block: a
  reader who can recompute a ``consistency_gate_results`` scope digest can
  recompute an E5 one with the same function.
* **The publication CHANNEL is not reachable from a batch plan, and the
  binding to it is a named deferral**
  (:data:`DEFERRED` ``reader_visible_channel_binding``). The only
  reader-visible channel today is the ``okg.consistency_gate_results``
  row read by ``okg generation describe``, and its NOT NULL keys are
  ``(run_id, check_id, target_generation_id)`` — a completed check run
  against a published generation. (Its PRIMARY KEY is the surrogate
  ``result_id``; what this argument needs is only that those three
  columns are NOT NULL, so no row can exist without all three.) A batch
  plan is PRE-execution: it has
  an ``instance_id`` and a ``plan.digest``, no run, no check, and no
  generation, so there is no plan-time write path. The surface's
  ``scope_complement_recipe`` is also the wrong SHAPE: it emits
  "unevaluated = <universe> MINUS <ids>", a subtraction from a live graph
  population, while ``MayChange`` is a SUPERSET that must CONTAIN the
  derived closure. Emitting a subtraction over batch-plan locations would
  produce a sentence that means nothing.

So the E5 disclosure set itself stays where the frozen contract already
puts it — :attr:`~...batch_schedule.SchedulePlan.may_change`, validated
by :func:`~...batch_schedule_checker.check_schedule` — and NO second
``MayChange`` publication channel is created. What this module adds is
the GRADE, which is a different question from the claim.

The grade the checker cannot compute
------------------------------------
:func:`~...batch_schedule_checker.derive_may_change` checks that a
claimed disclosure set CONTAINS the checker's own conservative closure
over the instance's DECLARED effect summaries. That is containment
against a declaration. §11 E5 asks for something the checker structurally
cannot supply: ``MayChange`` precision and recall against EVENTUAL FULL
PROCESSING — what actually changes once the whole sealed timeline runs
out, including work no instance had heard of yet.

The corpus therefore carries two separate things per action:

* its DECLARED ``effects`` — a conservative summary, possibly
  :class:`~...batch_schedule.Unknown`, which is what the contract sees
  and what the checker reasons over; and
* its ``truth_locations`` — what execution actually changes, which is
  harness-only ground truth and is stripped before an instance is
  compiled.

A declaration that is unsound against that ground truth passes the
checker and FAILS the grade. That is the point: the two checks are
independent, and the directed under-disclosure mutant
(``mutated_workload``) proves it by shrinking a change's affected scope
until the checker still accepts and recall drops below one.

Determinism
-----------
Every draw goes through :class:`~okg.bench.synth.prf.SynthPrf`. There is
no ``random``. §11 E5's metric list DOES name "replan latency", so unlike
E4 this module can be handed a clock — but the clock is INJECTED
(:func:`run_report`'s ``clock`` argument), it defaults to a clock that
answers UNKNOWN, and its readings live in
:attr:`E5Report.replan_latency_ns`, which
:meth:`E5Report.to_canonical_dict` does not emit. Wall time therefore
cannot reach the report digest, and two runs produce a byte-equal report
whether a clock was injected or not.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, replace
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
    OUTCOME_SUCCESS,
    PRIORITY_MUST,
    PRIORITY_SHOULD,
    TERMINATION_EXHAUSTED,
    BatchInstance,
    SchedulePlan,
    Unknown,
    load_batch_instance,
    object_digest,
)
from okg.substrate.consistency.batch_schedule_baselines import (
    BaselineRefused,
    plan_dependency_tier_greedy,
    plan_fifo,
    plan_marginal_coverage_greedy,
)
from okg.substrate.consistency.batch_schedule_checker import (
    CAUSE_DEFERRED_MAY_REVEAL_MUST,
    CAUSE_MANDATORY_UNIT_UNCOVERED,
    CAUSE_UNKNOWN_REVELATION,
    CHECKER_VERSION,
    check_schedule,
)
from okg.substrate.consistency.batch_schedule_oracle import build_certificate
from okg.substrate.consistency.observations import (
    SCOPE_DIGEST_ALGORITHM,
    SCOPE_ID_LIMIT,
    scope_identity,
)
from okg.substrate.errors import SubstrateError

__all__ = [
    "CANDIDATES",
    "CASES",
    "CASE_COST_OVERRUN",
    "CASE_DYNAMIC_CONSUMER",
    "CASE_EFFECT_WIDENING",
    "CASE_NONE",
    "CASE_REVEALABLE_UNKNOWN",
    "CLAUSES",
    "COMPARISON_BINDING_FIELDS",
    "CLAUSE_CHECKER_REFUSAL_READ_AS_PASS",
    "CLAUSE_DISCLOSURE_MISSES_EVENTUAL_LOCATION",
    "CLAUSE_PUBLICATION_AFTER_REVEALABLE_MUST",
    "CLAUSE_REPLANNING_COST_DOMINATES",
    "CLAUSE_STALE_PLAN_CONTINUES_ADMISSION",
    "CONCLUSION_NOT_ESTABLISHED",
    "CONCLUSION_NOT_ESTABLISHED_INVALID",
    "CONCLUSION_REPLANNING_PAYS",
    "CONCLUSION_REPLANNING_DOES_NOT_PAY",
    "COST_UNIT",
    "DEFERRED",
    "DISCLOSURE_OWNERSHIP",
    "DISCLOSURE_SCOPE_NAME",
    "MAX_EPOCHS",
    "MINIMUM_ADMISSION_STOP_PROBES",
    "MINIMUM_GRADED_PUBLICATIONS",
    "MINIMUM_COST_ACCOUNTED_RUNS",
    "PARTIAL_E5_DEFERRALS",
    "PLANNING_COST_PER_ACTION",
    "POLICIES",
    "POLICY_ROLLING_EPOCHS",
    "POLICY_STAGED_BARRIERS",
    "POLICY_STATIC_UNTIL_INVALIDATION",
    "PREREGISTERED_MANIFEST_DIGEST",
    "PRODUCER_FAMILY",
    "PUBLICATION_RULE_STRENGTHENING",
    "PRODUCER_VERSION",
    "QUALIFICATION_REFUSAL_CAUSES",
    "REPLAN_ROSTER_VERSION",
    "SCRIPTS",
    "SCRIPT_LATE_MUST",
    "SCRIPT_NEW_SHOULD_OR_MAY",
    "SCRIPT_NO_REVELATION",
    "SCRIPT_TWO_STEP_CASCADE",
    "STATUS_CHECKED_FEASIBLE",
    "STATUS_HARNESS_REFUSED",
    "STATUS_PLANNER_REFUSED",
    "STATUS_REFUSED",
    "UNIT_MAPPING_VERSION",
    "VERDICT_PASS",
    "VERDICT_PASS_PARTIAL_E5",
    "VERDICT_REFUSALS_SUFFIX",
    "WORKLOAD_NAMES",
    "ZERO_TOLERANCE",
    "AdmissionLedger",
    "AdmissionStopWitness",
    "BatchAdaptiveReplanningError",
    "CostAccount",
    "DisclosureGrade",
    "E5Manifest",
    "E5Report",
    "EpochRow",
    "FalsificationRow",
    "PolicyRun",
    "QualificationWitness",
    "StalePlanAdmission",
    "TimelineEvent",
    "Timeline",
    "Workload",
    "admission_stop_witness",
    "build_manifest",
    "build_timeline",
    "compile_instance",
    "cost_conclusion",
    "disclosure_identity",
    "eventual_world",
    "literal_publication_counterfactual",
    "mandatory_closure",
    "publications_with_outstanding_must",
    "run_policy",
    "run_report",
    "uncovered_mandatory_units",
    "workloads",
]


class BatchAdaptiveReplanningError(SubstrateError):
    """The harness refused. A mis-built experiment proves nothing."""


class StalePlanAdmission(BatchAdaptiveReplanningError):
    """An old plan tried to admit work after its digest went stale (§7).

    This is the enforcement §7 asks for, and it is an EXCEPTION rather
    than a report field on purpose: a policy that could keep going and
    merely be counted would make "all policies stop admitting old-plan
    work after invalidation" a measurement instead of the zero-tolerance
    clause §11 E5 declares it to be."""


# --------------------------------------------------------------------------- #
# The ONE local canonicalization.                                             #
# --------------------------------------------------------------------------- #
#: The two markers the local dialect adds to the trusted base's canonical
#: closure, and NOTHING else. They are objects rather than bare strings so
#: they cannot collide with a report field whose value really is the text
#: "true": the base's closure admits ``str`` and ``int``, so a bare
#: sentinel would be indistinguishable from real data. A mapping that
#: literally carried one of these keys WOULD collide, so the
#: canonicalizer refuses those keys by name.
#:
#: This is the SAME dialect the E4 lane defines for the same reason, with
#: this lane's own marker names — one dialect per lane, defined and
#: justified once, with every refusal the trusted base makes re-imposed
#: verbatim. It is not a second, wider dialect: floats (NaN and the
#: infinities included) refuse, unadmitted types refuse, and there is no
#: stringifying fallback of any kind.
_LOCAL_BOOL_KEY = "$e5_bool"
_LOCAL_ABSENT_KEY = "$e5_absent"
_LOCAL_RESERVED_KEYS = (_LOCAL_BOOL_KEY, _LOCAL_ABSENT_KEY)


def _local_canonical(value: Any, where: str = "$") -> Any:
    """Everything the trusted base's canonical closure takes, plus two.

    :func:`~...batch_schedule.canonical_json` admits integers and strings
    (inside mappings and sequences) and REFUSES ``None``, ``bool``,
    ``float``, and every other type. This module digests REPORT objects
    rather than contract objects, and a report row carries exactly two
    values that closure cannot take:

    * a **boolean** — a falsification row's ``fired``, and the
      ``*_ids_truncated`` flag the consumed disclosure vocabulary emits.
      Encoded as ``{"$e5_bool": "true"|"false"}``.
    * an **honest absence** — an objective tuple is ``None`` for an epoch
      whose plan the checker refused, which is a stated "not applicable",
      not an unknown quantity and not a zero. Encoded as
      ``{"$e5_absent": "not_stated"}``.

    Nothing else is admitted. A float in digested content is a digest
    that moves between machines, and a ``default=str`` fallback collides
    ``Fraction(1, 2)`` with the string ``"1/2"`` — so this function has no
    fallback at all."""
    if value is None:
        return dict([(_LOCAL_ABSENT_KEY, "not_stated")])
    if isinstance(value, bool):
        return dict([(_LOCAL_BOOL_KEY, "true" if value else "false")])
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return value
    if isinstance(value, float):
        raise BatchAdaptiveReplanningError(
            "the local digest dialect refuses floats exactly as the trusted "
            "base does; a float in digested content is a digest that moves "
            "between machines, and NaN is not even JSON",
            data={"where": where, "type": type(value).__name__},
        )
    if isinstance(value, Mapping):
        out: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise BatchAdaptiveReplanningError(
                    "mapping keys must be strings in the local digest dialect",
                    data={"where": where, "key_type": type(key).__name__},
                )
            if key in _LOCAL_RESERVED_KEYS:
                raise BatchAdaptiveReplanningError(
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
    raise BatchAdaptiveReplanningError(
        "the local digest dialect has NO stringifying fallback; an "
        "unadmitted type is refused, never rendered with str()",
        data={"where": where, "type": type(value).__name__},
    )


def _local_canonical_json(value: Any) -> str:
    """The single local canonical rendering. Every local digest uses it."""
    return json.dumps(
        _local_canonical(value), sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False,
    )


def _local_digest(value: Any) -> str:
    """sha256 over :func:`_local_canonical_json`."""
    return hashlib.sha256(
        _local_canonical_json(value).encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# §12 identity.                                                                #
# --------------------------------------------------------------------------- #
#: §12 producer identity. Bound to THIS experiment: a plan produced for E5
#: must not be traceable to E2's or E4's producer string.
PRODUCER_VERSION = "okg.bench.synth.batch-adaptive-replanning/1"
PRODUCER_FAMILY = "untrusted-baseline-family"

#: The candidate roster's own version. Two runs planned by different
#: rosters are not one experiment, and the comparison binding says so.
REPLAN_ROSTER_VERSION = "e5-roster/1"

COST_UNIT = "tokens"
UNIT_MAPPING_VERSION = "e5-unit-map/1"

#: The one named pool every compiled instance declares. E5 is not a
#: capacity experiment — E4 is — so the pool is a constant, and it exists
#: because a CARRIED RESERVATION that occupies nothing would not be a
#: reservation.
POOL = "pool"
POOL_CAPACITY = 2

#: The declared time unit. Durations, starts, ends, barriers, and idle
#: ticks are integers in it, exactly as §7 requires.
TIME_UNIT = "ticks"

#: The pinned conversion from planning work to the instance's single
#: declared operational cost unit (§7.1: "a version-pinned measurement or
#: conversion rule ... or an estimator derived from one of those"). One
#: cost unit per action a planning invocation must consider. It is a
#: DECLARED modelling choice, not a measurement — see
#: :data:`DEFERRED` ``planning_cost_conversion_is_declared_not_measured``
#: — and the report carries the BREAK-EVEN constant beside every verdict
#: so a reader can see how far the dominance answer sits from flipping.
PLANNING_COST_PER_ACTION = 1

#: The harness's own bound on how many epochs one timeline may take. §7's
#: closure rule terminates because every epoch completes at least one
#: action and every script has finitely many events, so this bound is a
#: guard against a corpus bug, not a cap on the experiment: hitting it
#: REFUSES the row by name rather than truncating it silently.
MAX_EPOCHS = 24


# --------------------------------------------------------------------------- #
# The candidate roster. Same untrusted proposal generators as E2 and E4,       #
# bound here to E5's own producer and roster identity.                         #
# --------------------------------------------------------------------------- #
CANDIDATES: tuple[tuple[str, Callable[[BatchInstance], SchedulePlan]], ...] = (
    ("fifo", plan_fifo),
    ("dependency_tier_greedy", plan_dependency_tier_greedy),
    ("marginal_coverage_greedy", plan_marginal_coverage_greedy),
    ("coverage_per_cost_greedy", plan_coverage_per_cost_greedy),
    ("tier_earliest_finish", plan_tier_earliest_finish),
)


# --------------------------------------------------------------------------- #
# §11 E5's four reveal scripts and the case grid.                              #
# --------------------------------------------------------------------------- #
#: §11 E5, verbatim: "for each sealed history, four reveal scripts: no
#: revelation; a new `should`/`may` after a check; a late `must`; and a
#: two-step cascade". A test enumerates these against that sentence, so a
#: script cannot be dropped without the drop being visible.
SCRIPT_NO_REVELATION = "no_revelation"
SCRIPT_NEW_SHOULD_OR_MAY = "new_should_or_may_after_a_check"
SCRIPT_LATE_MUST = "late_must"
SCRIPT_TWO_STEP_CASCADE = "two_step_cascade"
SCRIPTS: tuple[str, ...] = (
    SCRIPT_NO_REVELATION,
    SCRIPT_NEW_SHOULD_OR_MAY,
    SCRIPT_LATE_MUST,
    SCRIPT_TWO_STEP_CASCADE,
)

#: §11 E5's next sentence: "Include effect widening, cost overrun, and
#: dynamic-consumer cases." Those three are the case axis, plus a
#: no-modifier control and the §9 unknown-revelation case that the design
#: makes a REFUSAL rather than a widening.
CASE_NONE = "none"
CASE_EFFECT_WIDENING = "effect_widening"
CASE_COST_OVERRUN = "cost_overrun"
CASE_DYNAMIC_CONSUMER = "dynamic_consumer"
CASE_REVEALABLE_UNKNOWN = "revealable_unknown"
CASES: tuple[str, ...] = (
    CASE_NONE,
    CASE_EFFECT_WIDENING,
    CASE_COST_OVERRUN,
    CASE_DYNAMIC_CONSUMER,
    CASE_REVEALABLE_UNKNOWN,
)

#: The three admission policies §11 E5 names. All three stop admitting
#: old-plan work after invalidation — that is the zero-tolerance clause
#: :data:`CLAUSE_STALE_PLAN_CONTINUES_ADMISSION`, not a policy difference
#: — so they differ only in WHEN they replan and how far ahead they admit.
POLICY_STATIC_UNTIL_INVALIDATION = "stable_static_admission_until_invalidation"
POLICY_STAGED_BARRIERS = "staged_replanning_at_barriers"
POLICY_ROLLING_EPOCHS = "rolling_epoch_replanning"
POLICIES: tuple[str, ...] = (
    POLICY_STATIC_UNTIL_INVALIDATION,
    POLICY_STAGED_BARRIERS,
    POLICY_ROLLING_EPOCHS,
)

STATUS_CHECKED_FEASIBLE = "checked_feasible"
STATUS_REFUSED = "refused"
STATUS_PLANNER_REFUSED = "planner_refused"
STATUS_HARNESS_REFUSED = "harness_refused"

#: The checker causes that ARE a qualification refusal for mandatory or
#: revealable-mandatory work. Imported, never restated: a local copy of a
#: cause string would keep matching after the checker renamed it.
QUALIFICATION_REFUSAL_CAUSES: tuple[str, ...] = (
    CAUSE_DEFERRED_MAY_REVEAL_MUST,
    CAUSE_MANDATORY_UNIT_UNCOVERED,
    CAUSE_UNKNOWN_REVELATION,
)


# --------------------------------------------------------------------------- #
# The corpus. A workload says what work exists and what it TRULY changes;      #
# a script and a case say what execution reveals about it.                     #
# --------------------------------------------------------------------------- #
def _unit(name: str) -> dict[str, Any]:
    return {
        "spec_version": "e5-spec/1",
        "subject_scope_key": f"subject:{name}",
        "satisfaction_condition": f"condition:{name}",
    }


def _obligation(
    obligation_id: str, priority: str, unit_name: str, obligation_class: str,
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
    action_class: str,
    covers: Sequence[str] = (),
    requires: Sequence[str] = (),
    duration: int = 1,
    execution_cost: int = 1,
    truth_locations: Sequence[str] = (),
) -> dict[str, Any]:
    """One unit of work, with its DECLARED summary and its GROUND TRUTH.

    ``reveals`` and ``effects`` are the DECLARED summaries the contract
    sees; they default to a stated empty revelation and a stated location
    and are overwritten per script and per case. ``truth_locations`` is
    HARNESS-ONLY ground truth — what executing this action actually
    changes — and :func:`compile_instance` strips it before an instance is
    built. Keeping the two apart is what makes the disclosure GRADE
    independent of the disclosure CLAIM: a declaration that is unsound
    against the truth passes the checker and fails the grade.

    ``demand`` is absent here and written by :func:`compile_instance`,
    because §7 makes an omitted demand key a refusal and a workload that
    leaned on that refusal would be testing the loader, not replanning."""
    location = f"loc:{action_id}"
    return {
        "id": action_id,
        "action_class": action_class,
        "covers": list(covers),
        "requires": list(requires),
        "conflicts": [],
        "reveals": {"obligation_classes": [], "action_classes": []},
        "effects": {"locations": [location]},
        "duration": duration,
        "execution_cost": execution_cost,
        "cost_unit": COST_UNIT,
        "estimator": {"id": "e5-pinned-estimator", "version": 1},
        "truth_locations": list(truth_locations or (location,)),
    }


#: The three base obligations every workload declares, one per §6 tier.
_BASE_OBLIGATIONS: tuple[Mapping[str, Any], ...] = (
    _obligation("o_must", PRIORITY_MUST, "u_must", "base_must"),
    _obligation("o_should", PRIORITY_SHOULD, "u_should", "base_should"),
    _obligation("o_may", PRIORITY_MAY, "u_may", "base_may"),
)


@dataclass(frozen=True)
class Workload:
    """One batch of work, plus the reason it is in the corpus.

    A workload is held FIXED across the script, case, policy, and
    candidate grid. Its digest is a comparison-binding field: two rows
    compared against one another must be the same work.

    ``revealer`` is the action whose completion fires the SCRIPT's events
    and ``case_producer`` the action whose completion fires the CASE's, so
    the two axes are independent by construction rather than by luck.
    ``widen_target`` and ``overrun_target`` name the actions the widening
    and overrun cases act on — always work that runs LATER than the case
    producer, so the invalidation lands on an estimate that still
    matters."""

    name: str
    source: str
    exercises: str
    obligations: tuple[Mapping[str, Any], ...]
    actions: tuple[Mapping[str, Any], ...]
    revealer: str
    case_producer: str
    widen_target: str
    overrun_target: str
    #: The action with duplicate coverage, and the EXPENSIVE half of the
    #: duplicate pair. Both halves cover the same ``may`` unit, so §6
    #: credits the pair once and a cost-sensitive planner keeps the cheap
    #: one and DEFERS this one. That is the whole reason it exists: it is
    #: where a hazardous revelation declaration lands in a DEFERRED
    #: closure, which is what makes §9's unknown-revelation refusal and
    #: §8's revealable-`must` refusal reachable rather than theoretical.
    #: Making it the cheap half would invert the choice and leave both
    #: refusals unexercised — which is exactly what an earlier draft of
    #: this corpus did, and the checker-refusal census read empty.
    shadow: str

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source": self.source,
            "obligations": [dict(sorted(o.items()))
                            for o in sorted(self.obligations,
                                            key=lambda o: str(o["id"]))],
            "actions": [dict(sorted(a.items()))
                        for a in sorted(self.actions,
                                        key=lambda a: str(a["id"]))],
            "revealer": self.revealer,
            "case_producer": self.case_producer,
            "widen_target": self.widen_target,
            "overrun_target": self.overrun_target,
            "shadow": self.shadow,
        }

    @property
    def digest(self) -> str:
        return _local_digest(self.to_canonical_dict())

    def action_by_id(self, action_id: str) -> Mapping[str, Any] | None:
        for action in self.actions:
            if action["id"] == action_id:
                return action
        return None


#: The corpus names, in canonical order. A test enumerates the built
#: corpus against this tuple so a workload cannot be dropped silently.
WORKLOAD_NAMES: tuple[str, ...] = (
    "linear_chain",
    "shared_coverage",
    "deep_cascade",
)


def _linear_chain() -> Workload:
    return Workload(
        name="linear_chain",
        source="hand-built",
        exercises=(
            "a straight prerequisite chain, so an invalidation that lands "
            "mid-chain leaves exactly one action RUNNING and forces a "
            "carried fixed reservation into the next epoch"
        ),
        obligations=_BASE_OBLIGATIONS,
        actions=(
            _work("a_check", action_class="check", covers=["o_must"],
                  duration=2, execution_cost=3),
            _work("a_repair", action_class="repair", covers=["o_should"],
                  requires=["a_check"], duration=2, execution_cost=4),
            _work("a_probe", action_class="probe", covers=["o_may"],
                  duration=3, execution_cost=2),
            _work("a_shadow", action_class="shadow", covers=["o_may"],
                  duration=1, execution_cost=9),
        ),
        revealer="a_check",
        case_producer="a_probe",
        widen_target="a_repair",
        overrun_target="a_repair",
        shadow="a_shadow",
    )


def _shared_coverage() -> Workload:
    return Workload(
        name="shared_coverage",
        source="hand-built",
        exercises=(
            "one action covering two obligations across two tiers, plus a "
            "terminal gate, so a replan cannot re-earn coverage by "
            "re-selecting shared work and the §6 unit accounting is "
            "exercised across epoch boundaries"
        ),
        obligations=_BASE_OBLIGATIONS,
        actions=(
            _work("a_check", action_class="check",
                  covers=["o_must", "o_should"], duration=2,
                  execution_cost=5),
            _work("a_gate", action_class="gate", requires=["a_check"],
                  duration=2, execution_cost=3),
            _work("a_probe", action_class="probe", covers=["o_may"],
                  duration=2, execution_cost=2),
            _work("a_shadow", action_class="shadow", covers=["o_may"],
                  duration=1, execution_cost=9),
        ),
        revealer="a_check",
        case_producer="a_probe",
        widen_target="a_gate",
        overrun_target="a_gate",
        shadow="a_shadow",
    )


def _deep_cascade() -> Workload:
    return Workload(
        name="deep_cascade",
        source="hand-built",
        exercises=(
            "a three-deep chain, so the two-step cascade script has room "
            "to fire its second event in a LATER epoch than its first and "
            "a carried reservation can itself be carried again"
        ),
        obligations=_BASE_OBLIGATIONS,
        actions=(
            _work("a_check", action_class="check", covers=["o_must"],
                  duration=3, execution_cost=3),
            _work("a_repair", action_class="repair", covers=["o_should"],
                  requires=["a_check"], duration=2, execution_cost=4),
            _work("a_validate", action_class="validation",
                  requires=["a_repair"], duration=2, execution_cost=2),
            _work("a_probe", action_class="probe", covers=["o_may"],
                  duration=1, execution_cost=2),
            _work("a_shadow", action_class="shadow", covers=["o_may"],
                  duration=1, execution_cost=9),
        ),
        revealer="a_check",
        case_producer="a_probe",
        widen_target="a_repair",
        overrun_target="a_validate",
        shadow="a_shadow",
    )


def workloads() -> tuple[Workload, ...]:
    """The whole E5 corpus, in canonical order."""
    return (_linear_chain(), _shared_coverage(), _deep_cascade())


# --------------------------------------------------------------------------- #
# The sealed timeline. Events fire on the COMPLETION of a named action, never  #
# at a wall-clock tick, so the trigger is policy-independent by construction.  #
# --------------------------------------------------------------------------- #
EVENT_REVEAL = "reveal"
EVENT_EFFECT_WIDENING = "effect_widening"
EVENT_COST_OVERRUN = "cost_overrun"
EVENT_DYNAMIC_CONSUMER = "dynamic_consumer"


@dataclass(frozen=True)
class TimelineEvent:
    """One sealed execution result, and what §7 says it does to the epoch.

    Every event in this module is INVALIDATING: each one either reveals an
    obligation or invalidates an estimate, which are exactly the two
    triggers §7 names. There is no "informational" event, because an event
    that closed nothing would be an event this experiment is not about.

    ``produced_by`` is an ACTION id, not a time. An event fires when that
    action COMPLETES, so the same sealed timeline drives all three
    admission policies and the trigger cannot drift with the schedule a
    policy happens to produce."""

    index: int
    produced_by: str
    kind: str
    reveals_obligations: tuple[Mapping[str, Any], ...] = ()
    reveals_actions: tuple[Mapping[str, Any], ...] = ()
    #: Action ids whose DECLARED effect summary becomes UNKNOWN from the
    #: next instance on. §7: "An unknown effect widens disclosure to the
    #: whole affected scope."
    widens: tuple[str, ...] = ()
    #: Action id -> the corrected pinned execution cost. §7.1's third
    #: category: a realized measurement that invalidates an estimate.
    overruns: Mapping[str, int] = ()
    #: Already-executed action ids whose OBSERVATION this event
    #: invalidates. Their execution work becomes WASTED; the same ids, if
    #: they had not yet started, are work the admission stop SAVED.
    invalidates: tuple[str, ...] = ()
    detail: str = ""

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "produced_by": self.produced_by,
            "kind": self.kind,
            "reveals_obligations": [dict(sorted(o.items()))
                                    for o in self.reveals_obligations],
            "reveals_actions": [dict(sorted(a.items()))
                                for a in self.reveals_actions],
            "widens": list(self.widens),
            "overruns": dict(sorted(dict(self.overruns).items())),
            "invalidates": list(self.invalidates),
            "detail": self.detail,
        }


@dataclass(frozen=True)
class Timeline:
    """The sealed history of execution results for one (workload, script, case).

    Sealed means: built once from configuration and a
    :class:`~okg.bench.synth.prf.SynthPrf` draw stream, digested, and
    never consulted by a planner. Every policy sees the same events in the
    same order."""

    workload: str
    script: str
    case: str
    events: tuple[TimelineEvent, ...]

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "workload": self.workload,
            "script": self.script,
            "case": self.case,
            "events": [e.to_canonical_dict() for e in self.events],
        }

    @property
    def digest(self) -> str:
        return _local_digest(self.to_canonical_dict())

    @property
    def invalidated_ids(self) -> frozenset[str]:
        """Every action id any event in this timeline invalidates."""
        return frozenset(
            action_id for event in self.events
            for action_id in event.invalidates
        )


def _prf(workload: Workload, script: str, case: str) -> SynthPrf:
    return SynthPrf(f"e5:{workload.name}:{script}:{case}")


def build_timeline(workload: Workload, script: str, case: str) -> Timeline:
    """The sealed timeline for one (workload, reveal script, case).

    The SCRIPT's events fire on ``workload.revealer``; the CASE's fire on
    ``workload.case_producer``. The two producers are different actions in
    every workload, so a script and a case can both fire in one run
    without either masking the other."""
    if script not in SCRIPTS:
        raise BatchAdaptiveReplanningError(
            "unknown reveal script; the roster is closed",
            data={"script": script, "known": list(SCRIPTS)})
    if case not in CASES:
        raise BatchAdaptiveReplanningError(
            "unknown case; the roster is closed",
            data={"case": case, "known": list(CASES)})
    prf = _prf(workload, script, case)
    events: list[TimelineEvent] = []

    def _revealed(action_id: str, cls: str, covers: Sequence[str],
                  requires: Sequence[str] = ()) -> dict[str, Any]:
        return _work(
            action_id, action_class=cls, covers=covers, requires=requires,
            duration=prf.randint(1, 2, "duration", action_id),
            execution_cost=prf.randint(2, 4, "cost", action_id),
        )

    if script == SCRIPT_NEW_SHOULD_OR_MAY:
        events.append(TimelineEvent(
            index=len(events), produced_by=workload.revealer,
            kind=EVENT_REVEAL,
            reveals_obligations=(_obligation(
                "o_rev_should", PRIORITY_SHOULD, "u_rev_should",
                "rev_should"),),
            reveals_actions=(
                _revealed("a_rev", "rev_work", ["o_rev_should"]),),
            detail=("§11 E5's second script: a new `should` obligation "
                    "appears after a check has run"),
        ))
    elif script == SCRIPT_LATE_MUST:
        events.append(TimelineEvent(
            index=len(events), produced_by=workload.revealer,
            kind=EVENT_REVEAL,
            reveals_obligations=(_obligation(
                "o_rev_must", PRIORITY_MUST, "u_rev_must", "rev_must"),),
            reveals_actions=(
                _revealed("a_rev_must", "rev_work", ["o_rev_must"]),),
            detail=("§11 E5's third script: a LATE `must` appears after a "
                    "check has run, so the epoch that revealed it cannot "
                    "publish and the replan must cover it before any "
                    "epoch can"),
        ))
    elif script == SCRIPT_TWO_STEP_CASCADE:
        cascading = _revealed("a_rev", "rev_work", ["o_rev_should"])
        cascading["reveals"] = {
            "obligation_classes": ["casc_must"],
            "action_classes": ["casc_work"],
        }
        cascading["effects"] = {
            "unknown": ("this revealed action may itself reveal a mandatory "
                        "obligation, so its result locations are not "
                        "summarizable in advance")
        }
        events.append(TimelineEvent(
            index=len(events), produced_by=workload.revealer,
            kind=EVENT_REVEAL,
            reveals_obligations=(_obligation(
                "o_rev_should", PRIORITY_SHOULD, "u_rev_should",
                "rev_should"),),
            reveals_actions=(cascading,),
            detail="§11 E5's fourth script, step one of the cascade",
        ))
        events.append(TimelineEvent(
            index=len(events), produced_by="a_rev", kind=EVENT_REVEAL,
            reveals_obligations=(_obligation(
                "o_casc_must", PRIORITY_MUST, "u_casc_must", "casc_must"),),
            reveals_actions=(
                _revealed("a_casc", "casc_work", ["o_casc_must"]),),
            detail=("§11 E5's fourth script, step two: the work revealed by "
                    "step one reveals a `must` of its own, so a second "
                    "epoch closes on a revelation the first epoch could "
                    "not see"),
        ))

    if case == CASE_EFFECT_WIDENING:
        events.append(TimelineEvent(
            index=len(events), produced_by=workload.case_producer,
            kind=EVENT_EFFECT_WIDENING,
            widens=(workload.widen_target,),
            invalidates=(workload.case_producer, workload.widen_target),
            detail=("§11 E5's effect-widening case: execution shows the "
                    "declared effect summary was too narrow, so the "
                    "summary becomes UNKNOWN and disclosure widens to the "
                    "whole affected scope (§7)"),
        ))
    elif case == CASE_COST_OVERRUN:
        target = workload.action_by_id(workload.overrun_target)
        assert target is not None
        events.append(TimelineEvent(
            index=len(events), produced_by=workload.case_producer,
            kind=EVENT_COST_OVERRUN,
            overruns={
                workload.overrun_target: int(target["execution_cost"])
                + prf.randint(5, 9, "overrun", workload.overrun_target),
            },
            detail=("§11 E5's cost-overrun case: a realized measurement "
                    "invalidates the pinned estimate the plan was chosen "
                    "against (§7.1)"),
        ))
    elif case == CASE_DYNAMIC_CONSUMER:
        events.append(TimelineEvent(
            index=len(events), produced_by=workload.case_producer,
            kind=EVENT_DYNAMIC_CONSUMER,
            reveals_obligations=(_obligation(
                "o_dyn_should", PRIORITY_SHOULD, "u_dyn_should",
                "dyn_should"),),
            reveals_actions=(_revealed(
                "a_consumer", "dyn_consumer", ["o_dyn_should"],
                requires=[workload.widen_target]),),
            detail=("§11 E5's dynamic-consumer case: execution discovers a "
                    "consumer of work already in the batch, so the new "
                    "action arrives with a prerequisite edge into the "
                    "existing graph"),
        ))

    return Timeline(
        workload=workload.name, script=script, case=case,
        events=tuple(events),
    )


# --------------------------------------------------------------------------- #
# The world: DECLARED summaries per script and case, and its eventual state.  #
# --------------------------------------------------------------------------- #
_UNKNOWN_REVELATION_REASON = (
    "this action's revelation bound is not summarizable; §9 treats it as "
    "able to reveal every applicable configured `must`"
)
_UNKNOWN_EFFECT_REASON = (
    "this action may reveal work whose result locations do not exist yet, "
    "so its effect summary widens to the whole affected scope (§7)"
)


def _revealer_declaration(script: str) -> Mapping[str, Any]:
    """What the revealing action DECLARES about itself, per script.

    A revealer that can add work declares its effects UNKNOWN. That is not
    defensive padding: the locations that revealed work will touch do not
    exist in the instance yet, so no stated location set can bound them,
    and §7's answer to exactly that is to widen disclosure to the whole
    affected scope. The ``no_revelation`` script declares a stated empty
    revelation and a stated location, because there is nothing to widen
    for."""
    if script == SCRIPT_NO_REVELATION:
        return {"obligation_classes": [], "action_classes": []}
    if script == SCRIPT_NEW_SHOULD_OR_MAY:
        return {"obligation_classes": ["rev_should"],
                "action_classes": ["rev_work"]}
    if script == SCRIPT_LATE_MUST:
        return {"obligation_classes": ["rev_must"],
                "action_classes": ["rev_work"]}
    return {"obligation_classes": ["rev_should"],
            "action_classes": ["rev_work"]}


def _shadow_declaration(script: str, case: str) -> Mapping[str, Any]:
    """What the duplicate-coverage action declares about its revelation.

    This is the action a marginal planner DEFERS, so it is where a
    hazardous revelation becomes REACHABLE rather than theoretical:

    * under ``revealable_unknown`` its bound is UNKNOWN, and §9 refuses
      qualified publication outright when an UNKNOWN revelation sits
      anywhere in the deferred closure;
    * under a script whose revelation names a MANDATORY obligation class,
      it declares that class, and §8's feasibility condition fails when it
      is deferred.

    The case wins over the script when both apply, because UNKNOWN is the
    stricter of the two and a stricter reading may not be traded away for
    a more specific one."""
    if case == CASE_REVEALABLE_UNKNOWN:
        return {"unknown": _UNKNOWN_REVELATION_REASON}
    if script == SCRIPT_LATE_MUST:
        return {"obligation_classes": ["rev_must"], "action_classes": []}
    if script == SCRIPT_TWO_STEP_CASCADE:
        return {"obligation_classes": ["casc_must"], "action_classes": []}
    return {"obligation_classes": [], "action_classes": []}


def base_world(
    workload: Workload, script: str, case: str,
) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    """The epoch-zero obligations and actions, with declarations applied.

    Returns HARNESS records — they still carry ``truth_locations``.
    :func:`compile_instance` is the only thing that strips them."""
    obligations = [dict(o) for o in workload.obligations]
    actions: list[Mapping[str, Any]] = []
    for raw in workload.actions:
        action = dict(raw)
        if action["id"] == workload.revealer:
            action["reveals"] = dict(_revealer_declaration(script))
            if script != SCRIPT_NO_REVELATION:
                action["effects"] = {"unknown": _UNKNOWN_EFFECT_REASON}
        if action["id"] == workload.shadow:
            action["reveals"] = dict(_shadow_declaration(script, case))
        if action["id"] == workload.case_producer:
            if case == CASE_DYNAMIC_CONSUMER:
                action["reveals"] = {
                    "obligation_classes": ["dyn_should"],
                    "action_classes": ["dyn_consumer"],
                }
                action["effects"] = {"unknown": _UNKNOWN_EFFECT_REASON}
        actions.append(action)
    return obligations, actions


def _apply_event(
    obligations: list[Mapping[str, Any]],
    actions: list[Mapping[str, Any]],
    event: TimelineEvent,
) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    """Fold one fired execution result into the world.

    Additive for revelations, and REPLACING for a widening or an overrun:
    a corrected estimate replaces the pinned one it invalidated, which is
    §7.1's third category doing its job."""
    known_obligations = {str(o["id"]) for o in obligations}
    for obligation in event.reveals_obligations:
        if str(obligation["id"]) not in known_obligations:
            obligations.append(dict(obligation))
            known_obligations.add(str(obligation["id"]))
    known_actions = {str(a["id"]) for a in actions}
    for action in event.reveals_actions:
        if str(action["id"]) not in known_actions:
            actions.append(dict(action))
            known_actions.add(str(action["id"]))
    overruns = dict(event.overruns)
    updated: list[Mapping[str, Any]] = []
    for raw in actions:
        action = dict(raw)
        if action["id"] in event.widens:
            action["effects"] = {"unknown": _UNKNOWN_EFFECT_REASON}
        if action["id"] in overruns:
            action["execution_cost"] = overruns[str(action["id"])]
        updated.append(action)
    return obligations, updated


def eventual_world(
    workload: Workload, script: str, case: str,
) -> tuple[tuple[Mapping[str, Any], ...], tuple[Mapping[str, Any], ...]]:
    """The world after EVENTUAL FULL PROCESSING of the whole timeline.

    Every event fires, because full processing executes everything. This
    is the reference the disclosure grade is taken against and the source
    of each change's declared affected scope — so a corpus cannot declare
    a scope narrower than what its own timeline eventually touches without
    that being a deliberate, testable mutation."""
    timeline = build_timeline(workload, script, case)
    obligations, actions = base_world(workload, script, case)
    for event in timeline.events:
        obligations, actions = _apply_event(obligations, actions, event)
    return tuple(obligations), tuple(actions)


def eventual_locations(
    workload: Workload, script: str, case: str,
) -> tuple[str, ...]:
    """Ground truth: what full processing of this timeline actually changes.

    Taken over ``truth_locations``, NOT over the declared ``effects``. A
    conservative declaration widens the CLAIM; it does not move the
    truth, and grading a claim against a restatement of itself would
    grade nothing."""
    _, actions = eventual_world(workload, script, case)
    return tuple(sorted({
        str(location)
        for action in actions
        for location in action["truth_locations"]
    }))


# --------------------------------------------------------------------------- #
# Compiling one epoch's instance, carried reservations included.              #
# --------------------------------------------------------------------------- #
#: The id prefix a carried running action takes in the next epoch's
#: instance. It is a DIFFERENT identity from the action it continues,
#: because the original action's interval belongs to the epoch that
#: admitted it and the continuation belongs to the one that inherits it.
CARRY_PREFIX = "carry:"
CARRY_ACTION_CLASS = "carried_reservation"
CARRY_OBLIGATION_CLASS = "carried_reservation"


def _origin(action_id: str) -> str:
    """The original action a plan id refers to, carried or not."""
    if action_id.startswith(CARRY_PREFIX):
        return action_id[len(CARRY_PREFIX):]
    return action_id


def _contract_action(action: Mapping[str, Any]) -> dict[str, Any]:
    """Strip harness ground truth and write the demand §7 requires.

    ``truth_locations`` never reaches an instance: it is the reference the
    disclosure grade is taken against, and an instance that carried it
    would be handing the planner the answer sheet. The demand is written
    HERE rather than in the corpus because §7 declares ``demand(a, r)``
    for each named resource and an omitted key refuses — a workload that
    leaned on that refusal would be testing the loader."""
    out = {key: value for key, value in action.items()
           if key != "truth_locations"}
    out["demand"] = {POOL: 1}
    return out


def compile_instance(
    *,
    run_key: str,
    epoch: int,
    obligations: Sequence[Mapping[str, Any]],
    actions: Sequence[Mapping[str, Any]],
    carried: Sequence[Mapping[str, Any]],
    observation_digest: str,
    affected_scope: Sequence[str],
) -> dict[str, Any]:
    """One epoch's frozen instance, in the shape the loader takes.

    Carried running actions (§7's "carry already-running actions into the
    next instance as fixed reservations") are compiled into the plain
    frozen schema, exactly as the E4 lane compiles lanes away. There is no
    reservation field to set and this module does not invent one. A carry
    becomes:

    * an ACTION ``carry:<origin>`` with the origin's REMAINING duration,
      its demand, its declared effects and revelation, and no
      prerequisites — the ones it had already succeeded, which is why it
      was running;
    * covering everything the origin covered, PLUS a synthetic ``must``
      obligation of its own.

    That last clause is what makes the reservation FIXED in the only way
    the frozen contract can express and the checker can enforce: §8 makes
    every known ``must`` a hard feasibility condition, so a plan that
    defers a carried reservation is REFUSED by the independent checker
    with ``mandatory_unit_uncovered``. The harness does not grade the
    planner on it and does not rewrite the plan; the instance simply makes
    the alternative infeasible.

    What this encoding does NOT pin is the carry's START TIME. §7's
    schema has no release time and no deadline, so "it is already running,
    therefore it begins at zero" has nowhere to live; that residue is a
    named deferral (``fixed_reservation_start_time_not_expressible``) and
    the report measures how often a plan placed a carry at zero anyway,
    as a secondary metric that decides nothing.

    ``observation_digest`` binds the carried observations into
    ``instance_id``, which is inside the instance digest. §7 requires the
    next instance to "bind their eventual observations", and this is the
    binding: two epochs that inherited different observations cannot
    collide on a digest."""
    carried_origins = {str(c["origin"]) for c in carried}
    by_id = {str(a["id"]): a for a in actions}

    compiled: list[dict[str, Any]] = []
    for action in actions:
        if str(action["id"]) in carried_origins:
            continue
        record = dict(action)
        record["requires"] = [
            f"{CARRY_PREFIX}{r}" if r in carried_origins else r
            for r in record["requires"]
        ]
        compiled.append(_contract_action(record))

    carry_obligations: list[dict[str, Any]] = []
    for entry in sorted(carried, key=lambda c: str(c["origin"])):
        origin = str(entry["origin"])
        source = by_id.get(origin)
        if source is None:
            raise BatchAdaptiveReplanningError(
                "a carried reservation names an action the world does not "
                "declare; a reservation without its work is not a "
                "reservation",
                data={"origin": origin, "run_key": run_key})
        remaining = int(entry["remaining"])
        if remaining < 1:
            raise BatchAdaptiveReplanningError(
                "a carried reservation with no remaining duration is a "
                "COMPLETED action; §7 carries what is still RUNNING, and a "
                "zero-length interval satisfies no capacity check",
                data={"origin": origin, "remaining": remaining})
        carry_id = f"{CARRY_PREFIX}{origin}"
        carry_obligation_id = f"o_carry:{origin}"
        carry_obligations.append(_obligation(
            carry_obligation_id, PRIORITY_MUST, f"u_carry:{origin}",
            CARRY_OBLIGATION_CLASS))
        record = dict(source)
        record["id"] = carry_id
        record["action_class"] = CARRY_ACTION_CLASS
        record["covers"] = list(record["covers"]) + [carry_obligation_id]
        record["requires"] = []
        record["duration"] = remaining
        compiled.append(_contract_action(record))

    all_obligations = [dict(o) for o in obligations] + carry_obligations
    reservation = sum(int(a["duration"]) for a in compiled)
    return {
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "instance_id": f"e5/{run_key}/epoch{epoch}#{observation_digest[:16]}",
        "epoch": epoch,
        "cost_unit": COST_UNIT,
        "unit_mapping_version": UNIT_MAPPING_VERSION,
        "changes": [{
            "id": f"c:{run_key}",
            "affected_scope": list(affected_scope),
        }],
        "obligations": all_obligations,
        "actions": compiled,
        "conflict_groups": [],
        "resources": [{
            "id": POOL,
            "version": 1,
            "segments": [{"start": 0, "capacity": POOL_CAPACITY}],
            "reservation": reservation,
            "hard_cap": POOL_CAPACITY,
        }],
    }


def mandatory_closure(instance: BatchInstance) -> tuple[str, ...]:
    """The full prerequisite closure of every action covering a `must`.

    §6's rule, computed over the instance the checker will see. Used to
    define the staged policy's barriers and the delayed-mandatory metric,
    never to decide validity."""
    must_ids = {
        o.id for o in instance.obligations if o.priority == PRIORITY_MUST
    }
    closure = {
        a.id for a in instance.actions
        if any(o in must_ids for o in a.covers)
    }
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
    return tuple(sorted(closure))


# --------------------------------------------------------------------------- #
# The admission ledger: §7's enforcement, built here because the frozen        #
# contract carries `epoch` and enforces nothing.                               #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class _LiveEpoch:
    epoch: int
    instance_digest: str
    plan_digest: str
    stale: bool = False
    stale_reason: str = ""
    stale_at: int | None = None


class AdmissionLedger:
    """The ONE door every admission in this module goes through.

    §7: an execution result that reveals obligations or invalidates an
    estimate closes the epoch, and the old plan must "stop admitting
    not-yet-started actions". That sentence is enforced here, as a raised
    :class:`StalePlanAdmission` rather than a counter, for the reason §11
    E5 gives: "All policies stop admitting old-plan work after
    invalidation" is a zero-tolerance clause, not a policy difference, so
    no policy may be able to express the alternative.

    Three separate things refuse, and each names itself:

    * admitting against a plan digest that is not the live one — a plan
      from another epoch is not this epoch's plan;
    * admitting anything at all once the live plan has been invalidated;
      and
    * admitting an action that already started, because an action is
      non-preemptive (§7) and starting twice is not a schedule.

    Every refusal is RECORDED as well as raised, so a run can report how
    many times the stop actually bit rather than asserting that it would
    have."""

    def __init__(self) -> None:
        self._live: _LiveEpoch | None = None
        self._started: dict[str, int] = {}
        self._refusals: list[Mapping[str, Any]] = []
        self._admissions: list[Mapping[str, Any]] = []

    @property
    def live(self) -> _LiveEpoch | None:
        return self._live

    @property
    def stale(self) -> bool:
        return self._live is not None and self._live.stale

    @property
    def refusals(self) -> tuple[Mapping[str, Any], ...]:
        return tuple(self._refusals)

    @property
    def admissions(self) -> tuple[Mapping[str, Any], ...]:
        return tuple(self._admissions)

    def open_epoch(
        self, *, epoch: int, instance_digest: str, plan_digest: str,
    ) -> None:
        """Make one checked plan the live admission authority."""
        self._live = _LiveEpoch(
            epoch=epoch, instance_digest=instance_digest,
            plan_digest=plan_digest)

    def invalidate(self, *, at: int, reason: str) -> None:
        """Close the epoch: the live plan's digest is now STALE (§7)."""
        if self._live is None:
            raise BatchAdaptiveReplanningError(
                "cannot invalidate: no epoch is open",
                data={"at": at, "reason": reason})
        self._live = replace(
            self._live, stale=True, stale_reason=reason, stale_at=at)

    def admit(self, action_id: str, *, plan_digest: str, at: int) -> None:
        """Start one action under the live plan, or REFUSE by name."""
        live = self._live
        if live is None:
            self._refuse(action_id, at, "no_open_epoch",
                         "no epoch is open, so nothing may be admitted")
        assert live is not None
        if plan_digest != live.plan_digest:
            self._refuse(
                action_id, at, "plan_digest_not_live",
                "the admitting plan is not the live plan for this epoch; a "
                "plan from another epoch admits nothing")
        if live.stale:
            self._refuse(
                action_id, at, "plan_digest_stale",
                "the live plan's digest went stale when the epoch closed "
                f"at {live.stale_at} ({live.stale_reason}); §7 stops "
                "admission of not-yet-started actions from the old plan")
        if action_id in self._started:
            self._refuse(
                action_id, at, "already_started",
                "the action already started at "
                f"{self._started[action_id]}; §7 actions are non-preemptive "
                "and starting twice is not a schedule")
        self._started[action_id] = at
        self._admissions.append({
            "action_id": action_id, "at": at, "epoch": live.epoch,
            "plan_digest": live.plan_digest,
        })

    def _refuse(self, action_id: str, at: int, code: str, message: str) -> None:
        self._refusals.append({
            "action_id": action_id, "at": at, "code": code,
            "epoch": -1 if self._live is None else self._live.epoch,
        })
        raise StalePlanAdmission(
            f"admission refused [{code}] for {action_id!r} at {at}: {message}",
            data={"code": code, "action_id": action_id, "at": at})


# --------------------------------------------------------------------------- #
# The three admission policies (§11 E5).                                       #
# --------------------------------------------------------------------------- #
def _policy_horizon(
    policy: str, instance: BatchInstance, plan: SchedulePlan, makespan: int,
) -> int:
    """How far ahead this policy admits before it replans, in plan time.

    The policies differ ONLY here. Every one of them stops admitting on
    invalidation — that is enforced by :class:`AdmissionLedger` and is not
    a policy choice — so what is left to differ is how much work a policy
    commits to before it looks again:

    * ``stable_static_admission_until_invalidation`` commits to the WHOLE
      plan and looks again only when something invalidates it. Fewest
      planning invocations; most work exposed to a late revelation.
    * ``staged_replanning_at_barriers`` commits as far as the next
      BARRIER, defined as the earliest completion of a mandatory-closure
      action. An invalidation before the barrier stops admission at once
      and the replan waits for the barrier, so this policy — and only
      this policy — can leave the machine IDLE with mandatory work
      pending. That idle window is the delayed-mandatory metric.
    * ``rolling_epoch_replanning`` commits only to the next completion of
      anything. Most planning invocations; least work exposed."""
    if not plan.selected:
        return 0
    if policy == POLICY_STATIC_UNTIL_INVALIDATION:
        return makespan
    if policy == POLICY_ROLLING_EPOCHS:
        return min(s.end for s in plan.selected)
    if policy == POLICY_STAGED_BARRIERS:
        mandatory = set(mandatory_closure(instance))
        ends = sorted({s.end for s in plan.selected
                       if s.action_id in mandatory})
        return ends[0] if ends else makespan
    raise BatchAdaptiveReplanningError(
        "unknown admission policy; the roster is closed",
        data={"policy": policy, "known": list(POLICIES)})


def _producer(candidate: str, policy: str):
    """§12 producer identity, bound to E5's own roster AND policy."""
    from okg.substrate.consistency.batch_schedule import ProducerRef
    return ProducerRef(
        family=PRODUCER_FAMILY,
        version=PRODUCER_VERSION,
        parameters_digest=object_digest({
            "candidate": candidate,
            "policy": policy,
            "roster": REPLAN_ROSTER_VERSION,
        }),
        seed=Unknown(
            "every candidate on this roster is deterministic and draws no "
            "random value; the timeline seeds are the corpus's, not the "
            "planner's"),
        termination_status=TERMINATION_EXHAUSTED,
    )


def _plan_candidate(instance: BatchInstance, candidate: str) -> SchedulePlan:
    for name, planner in CANDIDATES:
        if name == candidate:
            return planner(instance)
    raise BatchAdaptiveReplanningError(
        "unknown candidate; the roster is closed",
        data={"candidate": candidate})


# --------------------------------------------------------------------------- #
# Disclosure identity: CONSUMED from the owning surface, not re-invented.      #
# --------------------------------------------------------------------------- #
#: The ``name`` prefix every E5 disclosure identity block is built under.
#: One name, so a reader addressing ``may_change_digest_sha256`` addresses
#: the same field in every row.
DISCLOSURE_SCOPE_NAME = "may_change"

#: What the ownership search found, recorded so the conclusion in the
#: module docstring is auditable rather than assertable. Echoed into the
#: preregistered manifest.
DISCLOSURE_OWNERSHIP: Mapping[str, str] = {
    "owner": (
        "design §3 assigns reader-visible identity of trimmed or "
        "unevaluated work to PACT `consistency-budget-and-disclosure` and "
        "says: reuse its disclosure surface, do not add a second "
        "MayChange publication channel"
    ),
    "searched": (
        "grep for the PACT id across pact/, src/, tests/, docs/; grep for "
        "MayChange / may_change / MAY_CHANGE across src/ and tests/; grep "
        "under src/okg/substrate/ for disclosure, deferred, unevaluated, "
        "trimmed, scope_disclosure, budget; read "
        "tests/substrate/consistency/test_scope_disclosure_contract.py and "
        "traced it to its implementation; traced every producer and every "
        "reader of the resulting row"
    ),
    "found_vocabulary": (
        "okg.substrate.consistency.observations.scope_identity / "
        "scope_digest / SCOPE_DIGEST_ALGORITHM / SCOPE_ID_LIMIT. The "
        "identity block is {name}_ids, {name}_count, {name}_ids_truncated, "
        "{name}_digest_sha256, {name}_digest_algorithm, and the digest is "
        "defined over sorted(str(v) for v in ids), so it is id-agnostic "
        "and CAN name a batch-plan location"
    ),
    "found_channel": (
        "the only reader-visible channel is the okg.consistency_gate_results "
        "row (src/okg/substrate/db/schema.sql), read by "
        "_generation_consistency_diagnostics in "
        "src/okg/substrate/cli/generation.py and surfaced by `okg "
        "generation describe`. Its NOT NULL keys are run_id (references "
        "derived_artifact_runs), check_id (references "
        "consistency_check_definitions), and target_generation_id"
    ),
    "decision": (
        "CONSUME the identity vocabulary; DEFER the channel binding. Every "
        "E5 disclosure row is built by calling scope_identity directly, so "
        "there is no second digest algorithm and no second identity block. "
        "The disclosure SET itself stays in the frozen schema's "
        "SchedulePlan.may_change, which the independent checker already "
        "validates, so no second MayChange publication channel is created"
    ),
    "missing": (
        "a batch plan is PRE-execution: it has an instance_id and a "
        "plan.digest and no run_id, no check_id, and no generation, so "
        "there is no plan-time write path into the reader-visible row. "
        "scope_complement_recipe is also the wrong shape — it emits "
        "'unevaluated = <universe> MINUS <ids>', a subtraction from a live "
        "graph population at a generation, while MayChange is a SUPERSET "
        "that must CONTAIN the derived closure. Closing the gap needs the "
        "owning PACT to accept a plan-scoped row, or needs the batch work "
        "to emit at execution time once a run exists; neither is this "
        "task's to own. All seven tasks of the owning PACT are status: "
        "todo, so the reader-visible surface described here is what exists "
        "today, not what that PACT promises"
    ),
}


@dataclass(frozen=True)
class DisclosureGrade:
    """One epoch's ``MayChange`` claim, graded against eventual processing.

    ``recall`` is the SOUNDNESS half and its threshold is one: every
    location that eventual full processing still changes must be inside
    the claim. ``precision`` is the COST OF CONSERVATISM and has no
    threshold at all — §7's widening rules are deliberately over-wide, so
    a low precision is the price of a sound recall being reported, not a
    failure.

    Both are carried UNREDUCED as numerator and denominator. A fraction
    reduced to a float would put a float in digested content, and a
    fraction reduced at all would hide the denominator that says how much
    was measured."""

    claimed: tuple[str, ...]
    eventual_remaining: tuple[str, ...]
    recall_numerator: int
    recall_denominator: int
    precision_numerator: int
    precision_denominator: int
    identity: Mapping[str, Any]

    @property
    def missing(self) -> tuple[str, ...]:
        """Locations eventual processing changes that the claim omits."""
        return tuple(sorted(set(self.eventual_remaining) - set(self.claimed)))

    @property
    def contained(self) -> bool:
        """Recall is exactly one. The zero-tolerance soundness clause."""
        return not self.missing

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "claimed": list(self.claimed),
            "eventual_remaining": list(self.eventual_remaining),
            "missing": list(self.missing),
            "recall": f"{self.recall_numerator}/{self.recall_denominator}",
            "precision": (
                f"{self.precision_numerator}/{self.precision_denominator}"),
            "identity": dict(sorted(self.identity.items())),
        }


def disclosure_identity(locations: Sequence[str]) -> Mapping[str, Any]:
    """The disclosure identity block, built by the OWNING surface's helper.

    This is the whole of E5's consumption of
    ``consistency-budget-and-disclosure``'s identity vocabulary: the ids
    are handed to :func:`~...observations.scope_identity` and the block it
    returns is emitted verbatim. E5 defines no digest algorithm, no
    truncation rule, and no identity field names of its own, so an E5 row
    and a consistency-gate row are recomputable by the same function.

    Ids are sorted before the call so a report is byte-stable: the helper
    sorts for the DIGEST but preserves input order for the id LIST."""
    return scope_identity(
        DISCLOSURE_SCOPE_NAME, sorted({str(item) for item in locations}))


def _grade_disclosure(
    claimed: Sequence[str], eventual_remaining: Sequence[str],
) -> DisclosureGrade:
    claim = sorted({str(c) for c in claimed})
    remaining = sorted({str(r) for r in eventual_remaining})
    hit = len(set(claim) & set(remaining))
    return DisclosureGrade(
        claimed=tuple(claim),
        eventual_remaining=tuple(remaining),
        recall_numerator=hit,
        recall_denominator=len(remaining),
        precision_numerator=hit,
        precision_denominator=len(claim),
        identity=disclosure_identity(claim),
    )


# --------------------------------------------------------------------------- #
# One (workload x script x case x policy x candidate) run.                     #
# --------------------------------------------------------------------------- #
DEFERRAL_NOT_APPLICABLE = ""
DEFERRED_CHECKER_REFUSED = "checker_refused"
DEFERRED_EPOCH_CLOSED_ON_INVALIDATION = "epoch_closed_on_invalidation"
DEFERRED_POLICY_BARRIER = "policy_barrier_epoch_not_final"


@dataclass(frozen=True)
class EpochRow:
    """One epoch: what was planned, what the checker said, what was admitted."""

    epoch: int
    instance_id: str
    instance_digest: str
    plan_digest: str
    status: str
    causes: tuple[str, ...]
    not_established: tuple[str, ...]
    actions_planned: int
    selected: tuple[str, ...]
    deferred: tuple[str, ...]
    carried_in: tuple[str, ...]
    carried_out: tuple[str, ...]
    carried_placed_at_zero: int
    admitted: tuple[str, ...]
    stale: bool
    stale_at: int | None
    admission_stop_probes: int
    admission_stop_refusals: int
    admitted_after_stale: tuple[str, ...]
    final: bool
    publication_qualified: bool
    publication_deferred_reason: str
    pending_must_revealable: tuple[str, ...]
    #: §8's hard feasibility condition, RE-DERIVED here from the plan's
    #: witnesses rather than read back from the checker. It is deliberately
    #: redundant with ``mandatory_unit_uncovered``: a second opinion that
    #: must agree is what could catch a gap in the first one, and grading a
    #: run by the engine's own decision would grade nothing.
    uncovered_must: tuple[str, ...]
    objective: Mapping[str, Any] | None
    planning_units: int
    idle_ticks: int
    grade: DisclosureGrade | None

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "epoch": self.epoch,
            "instance_id": self.instance_id,
            "instance_digest": self.instance_digest,
            "plan_digest": self.plan_digest,
            "status": self.status,
            "causes": list(self.causes),
            "not_established": list(self.not_established),
            "actions_planned": self.actions_planned,
            "selected": list(self.selected),
            "deferred": list(self.deferred),
            "carried_in": list(self.carried_in),
            "carried_out": list(self.carried_out),
            "carried_placed_at_zero": self.carried_placed_at_zero,
            "admitted": list(self.admitted),
            "stale": self.stale,
            "stale_at": self.stale_at,
            "admission_stop_probes": self.admission_stop_probes,
            "admission_stop_refusals": self.admission_stop_refusals,
            "admitted_after_stale": list(self.admitted_after_stale),
            "final": self.final,
            "publication_qualified": self.publication_qualified,
            "publication_deferred_reason": self.publication_deferred_reason,
            "pending_must_revealable": list(self.pending_must_revealable),
            "uncovered_must": list(self.uncovered_must),
            "objective": (None if self.objective is None
                          else dict(sorted(self.objective.items()))),
            "planning_units": self.planning_units,
            "idle_ticks": self.idle_ticks,
            "grade": (None if self.grade is None
                      else self.grade.to_canonical_dict()),
        }


@dataclass(frozen=True)
class PolicyRun:
    """One policy's whole trip through one sealed timeline."""

    workload: str
    workload_digest: str
    script: str
    case: str
    timeline_digest: str
    policy: str
    candidate: str
    roster_version: str
    manifest_digest: str
    status: str
    detail: str
    epochs: tuple[EpochRow, ...]
    planning_invocations: int
    planning_units: int
    executed_cost: int
    wasted_cost: int
    useful_cost: int
    saved_execution: int
    delayed_mandatory_ticks: int

    @property
    def replans(self) -> int:
        """Planning invocations after the first. The first is not a REPLAN."""
        return max(0, self.planning_invocations - 1)

    @property
    def planning_cost(self) -> int:
        """Planning work in the instance's declared cost unit (§7.1)."""
        return self.planning_units * PLANNING_COST_PER_ACTION

    @property
    def qualified_publications(self) -> tuple[EpochRow, ...]:
        return tuple(e for e in self.epochs if e.publication_qualified)

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "workload": self.workload,
            "workload_digest": self.workload_digest,
            "script": self.script,
            "case": self.case,
            "timeline_digest": self.timeline_digest,
            "policy": self.policy,
            "candidate": self.candidate,
            "roster_version": self.roster_version,
            "manifest_digest": self.manifest_digest,
            "status": self.status,
            "detail": self.detail,
            "epochs": [e.to_canonical_dict() for e in self.epochs],
            "replans": self.replans,
            "planning_invocations": self.planning_invocations,
            "planning_units": self.planning_units,
            "planning_cost": self.planning_cost,
            "executed_cost": self.executed_cost,
            "wasted_cost": self.wasted_cost,
            "useful_cost": self.useful_cost,
            "saved_execution": self.saved_execution,
            "delayed_mandatory_ticks": self.delayed_mandatory_ticks,
        }


def uncovered_mandatory_units(
    instance: BatchInstance, plan: SchedulePlan,
) -> tuple[str, ...]:
    """§8's hard feasibility condition, RE-DERIVED from the plan itself.

    Deliberately redundant with the checker's
    ``mandatory_unit_uncovered``: a grade computed from the engine's own
    decision grades nothing, so this walks the plan's witnesses directly
    and a disagreement between the two is observable. A unit is covered
    only when a witness reports success on a SELECTED action that also
    reports success; §6 counts UNITS, so cloned or split obligations
    collapse before anything is counted.

    Returns the obligation ids whose mandatory unit has no such witness."""
    must_units = set(instance.units_by_priority(PRIORITY_MUST))
    succeeded = {s.action_id for s in plan.selected
                 if s.outcome == OUTCOME_SUCCESS}
    covered: set[str] = set()
    for witness in plan.witnesses:
        if witness.outcome != OUTCOME_SUCCESS:
            continue
        if witness.action_id not in succeeded:
            continue
        obligation = instance.obligation_by_id(witness.obligation_id)
        if obligation is not None:
            covered.add(obligation.unit.key)
    uncovered = must_units - covered
    return tuple(sorted(
        o.id for o in instance.obligations
        if o.priority == PRIORITY_MUST and o.unit.key in uncovered))


def publications_with_outstanding_must(
    runs: Sequence[PolicyRun],
) -> tuple[str, ...]:
    """Every qualified publication that should have been DEFERRED.

    §11 E5's first falsifier, evaluated over BOTH of its halves, each of
    which can fire on its own:

    * KNOWN — the epoch's re-derived uncovered mandatory set is non-empty
      (:func:`uncovered_mandatory_units`); or
    * POSSIBLY REVEALABLE — the SEALED TIMELINE still holds an unfired
      event that would reveal a mandatory obligation.

    Neither half is read back from the checker, so a gap in the checker
    is observable here rather than echoed."""
    return tuple(sorted(
        f"{run.workload}/{run.script}/{run.case}/{run.policy}/"
        f"{run.candidate}@{row.epoch}"
        for run in runs for row in run.epochs
        if row.publication_qualified
        and (row.pending_must_revealable or row.uncovered_must)
    ))


def _pinned_cost(actions: Sequence[Mapping[str, Any]], origin: str) -> int:
    for action in actions:
        if str(action["id"]) == origin:
            return int(action["execution_cost"])
    raise BatchAdaptiveReplanningError(
        "no pinned cost for an executed action; an unmeasured action is "
        "never a free action",
        data={"action": origin})


def run_policy(
    workload: Workload,
    script: str,
    case: str,
    policy: str,
    candidate: str,
    *,
    manifest_digest: str,
    affected_scope: Sequence[str] | None = None,
    clock: Callable[[], int] | None = None,
) -> tuple[PolicyRun, list[int]]:
    """Drive one sealed timeline under one admission policy.

    The loop is §7's closure rule and nothing else:

    1. compile the REMAINING batch as this epoch's instance, with any
       already-running action carried in as a fixed reservation and the
       carried observations bound into the instance digest;
    2. ask the untrusted candidate for a plan and hand it to the
       INDEPENDENT CHECKER;
    3. admit selected actions through :class:`AdmissionLedger` up to the
       policy's horizon;
    4. if a completed action's sealed execution result reveals an
       obligation or invalidates an estimate, CLOSE the epoch: mark the
       plan's digest stale, then attempt every not-yet-started action from
       the old plan and record that each one is refused;
    5. carry what was still running, fold the result into the world, and
       replan.

    Returns the run and the injected clock's readings, which are kept OUT
    of the run object so wall time cannot reach a digest."""
    timeline = build_timeline(workload, script, case)
    scope = (tuple(str(s) for s in affected_scope)
             if affected_scope is not None
             else eventual_locations(workload, script, case))
    _, final_actions = eventual_world(workload, script, case)
    truth_by_action = {
        str(a["id"]): tuple(sorted(str(loc) for loc in a["truth_locations"]))
        for a in final_actions
    }
    run_key = f"{workload.name}/{script}/{case}/{policy}/{candidate}"

    ledger = AdmissionLedger()
    obligations, actions = base_world(workload, script, case)
    carried: list[Mapping[str, Any]] = []
    #: origin action id -> the GLOBAL tick it finishes. A running action
    #: is physics, not a plan decision: once admitted it runs to its
    #: pinned duration whatever the next epoch's policy horizon is, which
    #: is exactly what makes a carried reservation FIXED.
    running: dict[str, int] = {}
    completed: dict[str, int] = {}
    fired: set[int] = set()
    saved_ids: set[str] = set()
    wasted_ids: set[str] = set()

    rows: list[EpochRow] = []
    latencies: list[int] = []
    epoch = 0
    epoch_start = 0
    planning_invocations = 0
    planning_units = 0
    executed_cost = 0
    saved_execution = 0
    delayed_mandatory = 0
    status = STATUS_CHECKED_FEASIBLE
    detail = "the timeline ran to a final epoch"

    while True:
        if epoch > MAX_EPOCHS:
            status = STATUS_HARNESS_REFUSED
            detail = (
                f"the run exceeded {MAX_EPOCHS} epochs. §7's closure rule "
                "terminates because every epoch completes at least one "
                "action, so this is a corpus defect and the row REFUSES "
                "rather than being truncated to a number")
            break

        discharged = {
            str(o["id"]) for o in obligations
            if any(str(o["id"]) in a["covers"] and str(a["id"]) in completed
                   for a in actions)
        }
        live_obligations = [dict(o) for o in obligations
                            if str(o["id"]) not in discharged]
        live_actions: list[Mapping[str, Any]] = []
        for raw in actions:
            if str(raw["id"]) in completed:
                continue
            record = dict(raw)
            record["requires"] = [r for r in record["requires"]
                                  if r not in completed]
            record["covers"] = [c for c in record["covers"]
                                if c not in discharged]
            live_actions.append(record)

        observation_digest = _local_digest({
            "completed": [[k, v] for k, v in sorted(completed.items())],
            "carried": [dict(sorted(c.items()))
                        for c in sorted(carried,
                                        key=lambda c: str(c["origin"]))],
            "fired": sorted(fired),
        })
        raw_instance = compile_instance(
            run_key=run_key, epoch=epoch, obligations=live_obligations,
            actions=live_actions, carried=carried,
            observation_digest=observation_digest, affected_scope=scope)
        instance = load_batch_instance(raw_instance)
        carried_in = tuple(sorted(f"{CARRY_PREFIX}{c['origin']}"
                                  for c in carried))

        planning_invocations += 1
        planning_units += len(instance.actions)
        started_ns = clock() if clock is not None else None
        try:
            plan = _plan_candidate(instance, candidate)
        except BaselineRefused as exc:
            status = STATUS_PLANNER_REFUSED
            detail = f"the candidate refused to propose a plan: {exc}"
            rows.append(EpochRow(
                epoch=epoch, instance_id=instance.instance_id,
                instance_digest=instance.digest, plan_digest="",
                status=STATUS_PLANNER_REFUSED, causes=(),
                not_established=(), actions_planned=len(instance.actions),
                selected=(), deferred=(), carried_in=carried_in,
                carried_out=(), carried_placed_at_zero=0, admitted=(),
                stale=False, stale_at=None, admission_stop_probes=0,
                admission_stop_refusals=0, admitted_after_stale=(),
                final=False, publication_qualified=False,
                publication_deferred_reason=STATUS_PLANNER_REFUSED,
                pending_must_revealable=(), uncovered_must=(),
                objective=None,
                planning_units=len(instance.actions), idle_ticks=0,
                grade=None))
            break
        if clock is not None and started_ns is not None:
            latencies.append(max(0, clock() - started_ns))

        certificate = build_certificate(
            instance, plan, producer=_producer(candidate, policy),
            optimality_artifact_digest=ARTIFACT_UNBOUND)
        result = check_schedule(instance, plan, certificate)
        ledger.open_epoch(epoch=epoch, instance_digest=instance.digest,
                          plan_digest=plan.digest)

        makespan = max((s.end for s in plan.selected), default=0)
        horizon = _policy_horizon(policy, instance, plan, makespan)
        pending_by_producer: dict[str, list[int]] = {}
        for index, event in enumerate(timeline.events):
            if index in fired:
                continue
            pending_by_producer.setdefault(event.produced_by, []).append(index)

        firing_ends = [
            s.end for s in plan.selected
            if s.start < horizon and s.end <= horizon
            and pending_by_producer.get(_origin(s.action_id))
        ]
        if firing_ends:
            close = min(firing_ends)
            stale = True
        else:
            close = horizon
            stale = False

        admitted: list[Any] = []
        if result.accepted:
            for scheduled in sorted(plan.selected,
                                    key=lambda s: (s.start, s.action_id)):
                if scheduled.action_id.startswith(CARRY_PREFIX):
                    # Already RUNNING. §7 carries it; it is not admitted a
                    # second time, and its interval in this plan is a
                    # feasibility claim the checker validates rather than
                    # an instruction the harness follows.
                    continue
                if scheduled.start < close:
                    ledger.admit(scheduled.action_id,
                                 plan_digest=plan.digest,
                                 at=epoch_start + scheduled.start)
                    admitted.append(scheduled)
                    running[_origin(scheduled.action_id)] = (
                        epoch_start + scheduled.end)

        stale_at = None
        if stale and result.accepted:
            stale_at = epoch_start + close
            ledger.invalidate(
                at=stale_at,
                reason=("an execution result revealed an obligation or "
                        "invalidated an estimate (§7)"))

        stop_probes = 0
        stop_refusals = 0
        admitted_after_stale: list[str] = []
        if stale and result.accepted:
            for scheduled in sorted(
                (s for s in plan.selected
                 if s.start >= close
                 and not s.action_id.startswith(CARRY_PREFIX)),
                key=lambda s: (s.start, s.action_id),
            ):
                stop_probes += 1
                try:
                    ledger.admit(scheduled.action_id,
                                 plan_digest=plan.digest,
                                 at=epoch_start + scheduled.start)
                except StalePlanAdmission:
                    stop_refusals += 1
                else:
                    admitted_after_stale.append(scheduled.action_id)

        closing_tick = epoch_start + close
        for origin in sorted(
            (o for o, ends_at in running.items() if ends_at <= closing_tick),
            key=lambda o: (running[o], o),
        ):
            completed[origin] = running.pop(origin)
            executed_cost += _pinned_cost(actions, origin)

        newly = [timeline.events[i] for i in sorted(
            set(range(len(timeline.events))) - fired)
            if timeline.events[i].produced_by in completed]
        for event in newly:
            fired.add(event.index)
            obligations, actions = _apply_event(obligations, actions, event)
            wasted_ids |= {a for a in event.invalidates if a in completed}

        if stale and result.accepted:
            for scheduled in plan.selected:
                origin = _origin(scheduled.action_id)
                if (scheduled.start >= close
                        and origin in timeline.invalidated_ids
                        and origin not in completed
                        and origin not in saved_ids):
                    saved_ids.add(origin)
                    saved_execution += _pinned_cost(actions, origin)

        idle = 0
        if (stale and result.accepted
                and policy == POLICY_STAGED_BARRIERS and horizon > close):
            idle = horizon - close
        still_discharged = {
            str(o["id"]) for o in obligations
            if any(str(o["id"]) in a["covers"] and str(a["id"]) in completed
                   for a in actions)
        }
        mandatory_pending = any(
            o["priority"] == PRIORITY_MUST
            and str(o["id"]) not in still_discharged
            for o in obligations)
        if mandatory_pending:
            delayed_mandatory += idle

        carried = [
            {"origin": origin, "remaining": running[origin] - closing_tick}
            for origin in sorted(running)
        ]
        carried_at_zero = sum(
            1 for s in plan.selected
            if s.action_id.startswith(CARRY_PREFIX) and s.start == 0)

        remaining_truth: set[str] = set()
        for action in final_actions:
            action_id = str(action["id"])
            if action_id in completed:
                continue
            remaining_truth |= set(truth_by_action[action_id])
        grade = _grade_disclosure(plan.may_change, sorted(remaining_truth))

        pending_must = tuple(sorted({
            str(o["id"])
            for index, event in enumerate(timeline.events)
            if index not in fired
            for o in event.reveals_obligations
            if o["priority"] == PRIORITY_MUST
        }))

        uncovered_must = uncovered_mandatory_units(instance, plan)

        final = (not stale) and close >= makespan and not running
        row_status = (STATUS_CHECKED_FEASIBLE if result.accepted
                      else STATUS_REFUSED)
        qualified = result.accepted and final
        if qualified:
            reason = DEFERRAL_NOT_APPLICABLE
        elif not result.accepted:
            reason = DEFERRED_CHECKER_REFUSED
        elif stale:
            reason = DEFERRED_EPOCH_CLOSED_ON_INVALIDATION
        else:
            reason = DEFERRED_POLICY_BARRIER

        rows.append(EpochRow(
            epoch=epoch, instance_id=instance.instance_id,
            instance_digest=instance.digest, plan_digest=plan.digest,
            status=row_status, causes=result.causes(),
            not_established=result.not_established,
            actions_planned=len(instance.actions),
            selected=tuple(sorted(s.action_id for s in plan.selected)),
            deferred=tuple(sorted(plan.deferred)),
            carried_in=carried_in,
            carried_out=tuple(sorted(f"{CARRY_PREFIX}{c['origin']}"
                                     for c in carried)),
            carried_placed_at_zero=carried_at_zero,
            admitted=tuple(sorted(s.action_id for s in admitted)),
            stale=stale and result.accepted, stale_at=stale_at,
            admission_stop_probes=stop_probes,
            admission_stop_refusals=stop_refusals,
            admitted_after_stale=tuple(sorted(admitted_after_stale)),
            final=final, publication_qualified=qualified,
            publication_deferred_reason=reason,
            pending_must_revealable=pending_must,
            uncovered_must=uncovered_must,
            objective=(None if result.recomputed_objective is None
                       else result.recomputed_objective.to_canonical_dict()),
            planning_units=len(instance.actions), idle_ticks=idle,
            grade=grade))

        if not result.accepted:
            status = STATUS_REFUSED
            detail = (
                "the INDEPENDENT CHECKER refused this epoch's plan, so no "
                "work was admitted and the run stops: an unchecked plan is "
                "not a plan, and executing one would make this harness the "
                "validity authority. Causes: " + ",".join(result.causes()))
            break
        if final:
            break
        epoch_start = epoch_start + close + idle
        epoch += 1

    wasted_cost = sum(_pinned_cost(actions, a) for a in sorted(wasted_ids))
    run = PolicyRun(
        workload=workload.name, workload_digest=workload.digest,
        script=script, case=case, timeline_digest=timeline.digest,
        policy=policy, candidate=candidate,
        roster_version=REPLAN_ROSTER_VERSION, manifest_digest=manifest_digest,
        status=status, detail=detail, epochs=tuple(rows),
        planning_invocations=planning_invocations,
        planning_units=planning_units, executed_cost=executed_cost,
        wasted_cost=wasted_cost, useful_cost=executed_cost - wasted_cost,
        saved_execution=saved_execution,
        delayed_mandatory_ticks=delayed_mandatory)
    return run, latencies


# --------------------------------------------------------------------------- #
# The preregistered clauses (§11 preamble, §11 E5's falsification sentence).   #
# --------------------------------------------------------------------------- #
#: The preregistered floor on the admission-stop probe denominator. There
#: are three workloads and three policies, and §11 E5's sentence is about
#: policies, so a claim that "all policies stop admitting old-plan work"
#: drawn from fewer than one real probe per (workload, policy) pair is a
#: claim about a shape, not about the policies. 3 x 3 = 9. At or below
#: zero — where "measured nothing" and "measured and passed" would be the
#: same number — the report REFUSES to call the run clear.
MINIMUM_ADMISSION_STOP_PROBES = 9

#: The preregistered floor on the graded-publication denominator, on the
#: same structure E4 uses: three of the corpus workloads at the exhaustive
#: five-candidate roster. 3 x 5 = 15.
#:
#: It is stated over ``graded_with_reference`` — the qualified
#: publications whose ``recall_denominator`` is NON-EMPTY — and not over
#: the total graded count. A publication that eventual full processing
#: leaves nothing to change is contained VACUOUSLY: it cannot distinguish
#: a sound disclosure from an empty one, so counting it toward the floor
#: would let the zero-tolerance recall clause be satisfied by rows that
#: decide nothing. Below the floor the recall clause reports NOT
#: EVALUATED, does not fire, and the verdict says
#: ``invalid:graded_reference_publication_denominator_below_minimum``
#: rather than publishing a clean recall over a measurement nobody made.
MINIMUM_GRADED_PUBLICATIONS = 15

#: The preregistered floor on the cost-dominance denominator. §11 E5's
#: clause says "across the tested workloads", so the denominator is every
#: (workload, policy, candidate) triple: 3 x 3 x 5 = 45.
MINIMUM_COST_ACCOUNTED_RUNS = 45

#: The one place where this lane tests a STRONGER proposition than the
#: §11 E5 sentence it names, declared as a deviation rather than buried in
#: a metric definition. It is preregistered: it is a manifest field, so it
#: is inside :data:`PREREGISTERED_MANIFEST_DIGEST` and cannot be added,
#: removed, or softened after an outcome without moving the pin in the
#: same diff.
PUBLICATION_RULE_STRENGTHENING = (
    "STRICTER THAN §11 E5 READ LITERALLY. §11 E5's clause defers "
    "publication 'after a known or possibly revealable `must`'. This lane "
    "defers publication at EVERY non-final epoch — an epoch that closed on "
    "any revelation, `must` or not, and an epoch a staged barrier has not "
    "yet run to its plan's makespan — and reports those as "
    "'epoch_closed_on_invalidation' and 'policy_barrier_epoch_not_final'. "
    "The extra rule is a STRENGTHENING and never a relaxation: it can only "
    "withhold a publication the literal reading would have allowed, so no "
    "falsifying event escapes through it. JUSTIFICATION, on §7's 'replan "
    "the REMAINING batch': at a non-final epoch the plan has deferred "
    "nothing, so its MayChange set is EMPTY BY CONSTRUCTION while the "
    "actions it selected but did not complete go on changing locations. "
    "MayChange was never designed to cover selected-but-incomplete work, "
    "so a publication there would be a disclosure about a batch that has "
    "not happened. COST OF THE STRENGTHENING, reported and not asserted: "
    "E5Report.literal_publication_counterfactual() counts, per branch, the "
    "epochs the literal reading would have published and how many of those "
    "carry a disclosure the eventual timeline escapes"
)

CLAUSE_PUBLICATION_AFTER_REVEALABLE_MUST = (
    "publication_after_known_or_revealable_must")
CLAUSE_STALE_PLAN_CONTINUES_ADMISSION = "stale_plan_continues_admission"
CLAUSE_DISCLOSURE_MISSES_EVENTUAL_LOCATION = (
    "disclosure_misses_eventual_changed_location")
CLAUSE_REPLANNING_COST_DOMINATES = "replanning_cost_dominates_saved_execution"
CLAUSE_CHECKER_REFUSAL_READ_AS_PASS = "checker_refusal_read_as_pass"

#: Every falsification clause this lane declares, with the text it is
#: reported under. A clause is reported whether it fires or not — a
#: falsifier nobody can see is not a falsifier — and each text states the
#: WHOLE preregistered proposition it is evaluated against, names both
#: halves where the proposition has two, and marks any deferred term
#: INSIDE the string rather than only in another object.
CLAUSES: Mapping[str, str] = {
    CLAUSE_PUBLICATION_AFTER_REVEALABLE_MUST: (
        "E5/§11: 'publication after a known or possibly revealable `must` "
        "is deferred'. The falsifying event is therefore a QUALIFIED "
        "PUBLICATION that happened anyway, and the proposition has TWO "
        "halves, both evaluated over every reported epoch, both "
        "INDEPENDENTLY of the checker so that a gap in the checker is "
        "observable rather than echoed. KNOWN: §8's hard feasibility "
        "condition is re-derived here from the plan's own witnesses — a "
        "`must` coverage unit with no successful witness on a successful "
        "action is uncovered — and a row that reports qualification with a "
        "non-empty uncovered set fires the clause. POSSIBLY REVEALABLE: "
        "the SEALED TIMELINE, which no instance and no checker can see, is "
        "consulted for an unfired event that would reveal a `must` "
        "obligation; a row that reports qualification while such an event "
        "is still pending fires the clause. A narrower check on either "
        "half alone could not observe the other: the checker's own "
        "refusal causes are reported beside these, and a qualified row "
        "carries none by construction, so reading only those causes would "
        "make this clause a restatement of the checker rather than a test "
        "of it. STRONGER THAN THE LITERAL SENTENCE, and named as such: "
        "publication is additionally withheld — reported as a deferral "
        "reason rather than as a firing — on BOTH of two branches, "
        "'epoch_closed_on_invalidation' (the epoch closed on a revelation "
        "of any kind, because §7 says such a result CLOSES the epoch and "
        "an epoch that must replan has not finished) and "
        "'policy_barrier_epoch_not_final' (a staged barrier has not run "
        "the epoch to its plan's makespan). Neither branch is required by "
        "§11 E5 read literally; both can only WITHHOLD a publication, so "
        "the clause remains at least as strong as the sentence it names. "
        "See manifest.publication_rule_strengthening for the §7 "
        "justification and E5Report.literal_publication_counterfactual() "
        "for what the literal reading would have published on each branch. "
        "Zero tolerance"
    ),
    CLAUSE_STALE_PLAN_CONTINUES_ADMISSION: (
        "E5/§11: 'an old plan continues admission after its digest becomes "
        "stale'. Evaluated over every not-yet-started action of every "
        "invalidated plan, in every policy, by ATTEMPTING the admission "
        "against the live ledger after the epoch closed and recording what "
        "happened. §11 E5 says 'All policies stop admitting old-plan work "
        "after invalidation', so this is a zero-tolerance clause and not a "
        "policy difference: the stop is enforced by AdmissionLedger, which "
        "RAISES rather than counts, and a policy has no other door through "
        "which to admit. The clause fires when any attempt SUCCEEDS. "
        "[DEFERRED: epoch_enforcement_is_experiment_layer_only — the frozen "
        "contract carries BatchInstance.epoch and digests it and enforces "
        "nothing about it; every stop measured here is this harness's, and "
        "no contract guarantee is claimed.] Zero tolerance"
    ),
    CLAUSE_DISCLOSURE_MISSES_EVENTUAL_LOCATION: (
        "E5/§11: 'disclosure misses an eventual changed location'. "
        "Evaluated over every QUALIFIED PUBLICATION — the epochs at which "
        "a disclosure set is actually published — by comparing the plan's "
        "claimed MayChange set against EVENTUAL FULL PROCESSING of the "
        "whole sealed timeline, restricted to the actions that epoch did "
        "not complete. Eventual change is taken over the corpus's "
        "harness-only truth_locations, NOT over the declared effect "
        "summaries, so the grade is independent of the claim and of the "
        "checker: the checker validates that a claim CONTAINS its own "
        "conservative closure over DECLARATIONS, which a corpus with an "
        "unsound declaration passes. RECALL is the soundness half and its "
        "threshold is ONE (containment). PRECISION is reported as the cost "
        "of conservatism and has NO threshold, because §7's widening rules "
        "are deliberately over-wide. The same grade over every "
        "checked-feasible epoch, published or not, is reported as a "
        "SECONDARY metric and decides nothing: a mid-timeline epoch's "
        "disclosure is not a publication surface. Zero tolerance on recall"
    ),
    CLAUSE_REPLANNING_COST_DOMINATES: (
        "E5/§11: 'replanning cost dominates saved execution across the "
        "tested workloads'. Evaluated across every (workload, script, "
        "case, policy, candidate) run that reached a cost account, in the "
        "instance's SINGLE declared cost unit (§7.1). PLANNING COST is "
        "planning_units — the number of actions each planning invocation "
        "had to consider, summed over invocations — converted at the "
        "pinned rate PLANNING_COST_PER_ACTION [DEFERRED: "
        "planning_cost_conversion_is_declared_not_measured — the rate is a "
        "declared modelling choice, not a measurement, so the report "
        "carries the BREAK-EVEN rate beside this verdict and a reader can "
        "see how far the answer sits from flipping]. SAVED EXECUTION is "
        "the pinned execution cost of old-plan actions that the admission "
        "stop prevented from starting AND that the sealed timeline goes on "
        "to invalidate — that is, exactly the work replanning bought, "
        "counted once per action. The clause fires when total planning "
        "cost EXCEEDS total saved execution. It is NOT zero tolerance: "
        "firing rejects the claim that replanning pays on this corpus at "
        "this rate; it does not refute epoch invalidation, whose safety "
        "clauses are the three zero-tolerance ones"
    ),
    CLAUSE_CHECKER_REFUSAL_READ_AS_PASS: (
        "E5: a checker refusal read as a pass. Evaluated structurally over "
        "every reported epoch row: a row may report a qualified "
        "publication only when the INDEPENDENT CHECKER's own verdict was "
        "checked_feasible with no findings, and a row that reported "
        "qualification while the checker refused would be this harness "
        "overriding the only validity authority it has. Zero tolerance"
    ),
}

#: The zero-tolerance subset (§11 preamble). Any one firing refutes the
#: slice. The cost clause rejects the replanning-pays CLAIM without
#: refuting the mechanism, which is what §11 E5's own framing implies.
ZERO_TOLERANCE: tuple[str, ...] = (
    CLAUSE_CHECKER_REFUSAL_READ_AS_PASS,
    CLAUSE_DISCLOSURE_MISSES_EVENTUAL_LOCATION,
    CLAUSE_PUBLICATION_AFTER_REVEALABLE_MUST,
    CLAUSE_STALE_PLAN_CONTINUES_ADMISSION,
)

#: The fields two runs must AGREE on before their cost accounts may be
#: compared. A per-run sanity check does not catch a cross-run mismatch,
#: so the comparison refuses the pair by name instead of quietly
#: comparing two different experiments.
COMPARISON_BINDING_FIELDS: tuple[str, ...] = (
    "candidate",
    "manifest_digest",
    "roster_version",
    "timeline_digest",
    "workload_digest",
)

#: The named deferrals. Each one is carried in the manifest and echoed in
#: the report: a deferral is a row, never a silent cap.
DEFERRED: Mapping[str, str] = {
    "epoch_enforcement_is_experiment_layer_only": (
        "The frozen contract CARRIES BatchInstance.epoch and digests it and "
        "does nothing else with it: batch_schedule_checker has no epoch "
        "cause, no epoch guarantee, and nothing in not_established about "
        "one, and it is right not to claim any. Every epoch closure, "
        "admission stop, and carried reservation measured here is enforced "
        "by THIS module's AdmissionLedger and by compiling the reservation "
        "into an ordinary mandatory obligation the checker can refuse. "
        "Nothing in this report is evidence that the contract enforces an "
        "epoch, and no runtime may read it that way. Moving the "
        "enforcement into the contract is a schema and checker change this "
        "task does not own"
    ),
    "reader_visible_channel_binding": (
        "Design §3 says to reuse `consistency-budget-and-disclosure`'s "
        "disclosure surface and add no second MayChange publication "
        "channel. Its identity VOCABULARY is consumed verbatim here — "
        "scope_identity / scope_digest / SCOPE_DIGEST_ALGORITHM from "
        "okg.substrate.consistency.observations build every E5 disclosure "
        "row, so there is no second digest algorithm and no second "
        "identity block — and the disclosure SET stays in the frozen "
        "schema's SchedulePlan.may_change, which the independent checker "
        "already validates, so no second publication channel is created. "
        "What is DEFERRED is the binding to the reader-visible CHANNEL. "
        "The only such channel today is the okg.consistency_gate_results "
        "row read by `okg generation describe`, whose NOT NULL keys are "
        "run_id, check_id, and target_generation_id. A batch plan is "
        "PRE-execution: it has an instance_id and a plan.digest and no "
        "run, no check, and no generation, so there is no plan-time write "
        "path. The surface's scope_complement_recipe is also the wrong "
        "shape — it emits 'unevaluated = <universe> MINUS <ids>', a "
        "subtraction from a live graph population, while MayChange is a "
        "SUPERSET that must CONTAIN the derived closure — so reusing it "
        "would emit a sentence that means nothing. Closing the gap needs "
        "the owning PACT to accept a plan-scoped row or needs emission to "
        "move to execution time once a run exists; all seven of that "
        "PACT's tasks are status: todo. See DISCLOSURE_OWNERSHIP for the "
        "full search"
    ),
    "fixed_reservation_start_time_not_expressible": (
        "§7 says a carried running action becomes a FIXED reservation. The "
        "frozen schema can express that it MUST run (a mandatory "
        "obligation the checker refuses a plan for deferring), for how "
        "long (its remaining duration), and against what capacity (its "
        "demand). It cannot express that it BEGINS AT ZERO, because "
        "Action carries no release time and no deadline and the plan "
        "carries no fixed-start field. The residue is reported rather than "
        "faked: carried_reservation_placed_at_zero counts how often a plan "
        "placed a carry at zero anyway, as a SECONDARY metric that decides "
        "nothing. Adding a release time is a schema change this task does "
        "not own"
    ),
    "carried_reservation_cost_charged_once_at_completion": (
        "Execution cost is charged ONCE, at completion, keyed by the "
        "ORIGINAL action id, so an action carried across two epochs is not "
        "billed twice. The consequence is that the carry ALSO contributes "
        "its full pinned cost to the objective of every epoch that plans "
        "it, because the frozen objective sums the pinned cost of selected "
        "actions and has no notion of work already done. Objective totals "
        "across epochs are therefore not additive, and objective_change_by_"
        "epoch is reported per epoch rather than summed"
    ),
    "planning_cost_conversion_is_declared_not_measured": (
        "§7.1 permits 'an estimator derived from one of those' with a "
        "version-pinned conversion rule, and PLANNING_COST_PER_ACTION is "
        "that rule: one declared cost unit per action a planning "
        "invocation must consider. It is a DECLARED modelling choice and "
        "not a measurement of anything, so the dominance clause's answer "
        "moves with it. The report therefore carries the BREAK-EVEN rate "
        "beside the verdict, so a reader sees how far the answer is from "
        "flipping instead of having to trust the constant. Measuring real "
        "planning work belongs to the measurement adapter's live path"
    ),
    "replan_latency_wall_time_not_digested": (
        "§11 E5's metric list names 'replan latency'. A clock CAN be "
        "injected into run_report and its readings are returned in "
        "E5Report.replan_latency_ns, but to_canonical_dict does not emit "
        "them and the report digest cannot move with them: wall time in "
        "digested content is a digest that moves between machines. Under "
        "the default no-clock configuration the latency is UNKNOWN with a "
        "reason, never a zero. The DETERMINISTIC surrogate — the planning "
        "units each replan had to consider — IS digested and is what the "
        "cost clause uses"
    ),
    "realized_execution_measurements": (
        "§7.1 separates estimated, reserved, and REALIZED quantities. "
        "Nothing is executed here: every cost, duration, and tick is a "
        "PINNED estimate validated by the checker, and the sealed "
        "timeline's cost-overrun events are declared corrections rather "
        "than measured ones. 'Useful and wasted work' is therefore "
        "evaluated in pinned units over a checked plan. The realized half "
        "belongs to the measurement adapter's live path"
    ),
    "history_window_batches": (
        "§11 E5's data clause is 'for each sealed history, four reveal "
        "scripts'. The deterministic 22-update histories replayed in "
        "windows of 1, 4, and 8 need the database-backed "
        "corpus-to-instance binding, which does not exist yet. DEFERRED, "
        "exactly as the E2 and E4 lanes defer it; the hand-built "
        "workloads with their PRF-drawn sealed timelines are the data "
        "actually used, and all four reveal scripts run against every one "
        "of them"
    ),
    "preemption": (
        "§7 makes actions non-preemptive unless the instance decomposes "
        "them into explicit execution chunks with their own identities, "
        "dependencies, effects, and demands, and §11 E5 declares no "
        "preemption policy. A carried reservation is NOT a preemption: the "
        "action was never stopped, and the carry is the same work "
        "continuing with a shorter remaining interval. Nothing here "
        "preempts, and the chunk decomposition the schema does offer is "
        "DEFERRED rather than approximated"
    ),
    "single_pool_capacity_not_varied": (
        "E5 holds the named pool fixed at one capacity. §11 E5's metric "
        "list contains no per-resource utilisation, no queue delay, and no "
        "capacity comparison — that whole question is E4's, and it runs "
        "there over the full configuration grid. The pool exists here "
        "because a carried reservation that occupied nothing would not be "
        "a reservation, not because this lane is measuring capacity"
    ),
    "optimality_reference": (
        "No exhaustive-oracle arm runs. §11 E5's metric list names no "
        "gap-to-optimum, §9.1 permits the word 'optimal' only through a "
        "checked artifact, and §7 says G5 never crosses epochs — which is "
        "precisely what a multi-epoch run does. Every number here is a "
        "CHECKED FEASIBLE candidate's checked value"
    ),
}

#: The deferrals that make this run PARTIAL E5 rather than E5. Each one
#: subtracts from the experiment §11 E5 declares — its data clause, the
#: word 'realized', a metric that cannot reach the digest, or the fact
#: that the epoch enforcement is the harness's and not the contract's — so
#: a clear run of what DID execute is not the sentence "E5 passed".
PARTIAL_E5_DEFERRALS: tuple[str, ...] = (
    "epoch_enforcement_is_experiment_layer_only",
    "history_window_batches",
    "realized_execution_measurements",
    "replan_latency_wall_time_not_digested",
)

VERDICT_PASS = "pass"
VERDICT_PASS_PARTIAL_E5 = "pass_partial_e5"

#: The two derived conclusions on E5's cost question. DERIVED from the
#: preregistered comparison, never authored.
CONCLUSION_REPLANNING_PAYS = "replanning_saves_more_execution_than_it_costs"
#: The dominance conclusion names BOTH of its conditionalities. The
#: corpus is one premise; the DECLARED conversion rate
#: (:data:`PLANNING_COST_PER_ACTION`, a modelling choice and not a
#: measurement — see ``deferred.planning_cost_conversion_is_declared_not_
#: measured``) is the other, and a token that named only the first would
#: let a reader carry the sentence away without the rate that produced
#: it. How far the answer sits from flipping is a separate, DERIVED
#: statement: :attr:`CostAccount.rate_sensitivity` reads it off the
#: break-even rate and travels beside the conclusion.
CONCLUSION_REPLANNING_DOES_NOT_PAY = (
    "replanning_cost_dominates_saved_execution_on_this_corpus_at_the_"
    "declared_conversion_rate")

#: The third conclusion, and the reason there has to be one: a comparison
#: over too few accounted runs is not a negative result about replanning,
#: it is the absence of a measurement.
CONCLUSION_NOT_ESTABLISHED = "not_established:cost_denominator_below_minimum"
#: And a conclusion may not be published at all beneath an `invalid:`
#: verdict, whatever the denominator was.
CONCLUSION_NOT_ESTABLISHED_INVALID = "not_established:report_invalid"

#: The suffix a derived verdict carries when any run REFUSED. A shrunken
#: experiment must be loud in the verdict, not inferable from a
#: denominator.
VERDICT_REFUSALS_SUFFIX = "with_refusals"


# --------------------------------------------------------------------------- #
# The preregistered manifest.                                                  #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class E5Manifest:
    """The preregistered E5 manifest, frozen before outcomes.

    Built by :func:`build_manifest` from CONFIGURATION ONLY. It takes no
    result object of any kind, so a manifest cannot be shaped by what the
    run produced; :meth:`digest` binds every report row."""

    workloads: tuple[str, ...]
    reveal_scripts: tuple[str, ...]
    design_named_reveal_scripts: tuple[str, ...]
    cases: tuple[str, ...]
    policies: tuple[str, ...]
    candidates: tuple[str, ...]
    roster_version: str
    comparison_binding_fields: tuple[str, ...]
    epoch_semantics: Mapping[str, str]
    disclosure_ownership: Mapping[str, str]
    #: The declared deviation: where the tested proposition is STRONGER
    #: than the §11 E5 sentence the lane names, with its §7 justification.
    #: A deviation from the clause under test is not a metric definition,
    #: so it gets its own preregistered field.
    publication_rule_strengthening: str
    metric_definitions: Mapping[str, str]
    #: Metrics that are REPORTED and compared but decide nothing. Named in
    #: the preregistration so a reader can tell a secondary surface from
    #: the decider without reading ``primary_metric`` first.
    secondary_metrics: Mapping[str, str]
    cost_unit: str
    time_unit: str
    planning_cost_per_action: int
    max_epochs: int
    minimum_admission_stop_probes: int
    minimum_graded_publications: int
    minimum_cost_accounted_runs: int
    zero_tolerance: Mapping[str, str]
    falsification_clauses: Mapping[str, str]
    primary_metric: str
    comparator: str
    aggregation: str
    uncertainty: str
    threshold: str
    deferred: Mapping[str, str]
    schema: str = "okg.synth.e5-adaptive-replanning.v1"

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "workloads": list(self.workloads),
            "reveal_scripts": list(self.reveal_scripts),
            "design_named_reveal_scripts": list(
                self.design_named_reveal_scripts),
            "cases": list(self.cases),
            "policies": list(self.policies),
            "candidates": list(self.candidates),
            "roster_version": self.roster_version,
            "comparison_binding_fields": list(self.comparison_binding_fields),
            "epoch_semantics": dict(sorted(self.epoch_semantics.items())),
            "disclosure_ownership": dict(
                sorted(self.disclosure_ownership.items())),
            "publication_rule_strengthening": (
                self.publication_rule_strengthening),
            "metric_definitions": dict(sorted(self.metric_definitions.items())),
            "secondary_metrics": dict(sorted(self.secondary_metrics.items())),
            "cost_unit": self.cost_unit,
            "time_unit": self.time_unit,
            "planning_cost_per_action": self.planning_cost_per_action,
            "max_epochs": self.max_epochs,
            "minimum_admission_stop_probes":
                self.minimum_admission_stop_probes,
            "minimum_graded_publications": self.minimum_graded_publications,
            "minimum_cost_accounted_runs": self.minimum_cost_accounted_runs,
            "zero_tolerance": dict(sorted(self.zero_tolerance.items())),
            "falsification_clauses": dict(
                sorted(self.falsification_clauses.items())),
            "primary_metric": self.primary_metric,
            "comparator": self.comparator,
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


def build_manifest(
    *,
    workload_names: Sequence[str] = WORKLOAD_NAMES,
    scripts: Sequence[str] = SCRIPTS,
    cases: Sequence[str] = CASES,
    policies: Sequence[str] = POLICIES,
) -> E5Manifest:
    """The one manifest constructor: configuration in, preregistration out."""
    if not workload_names:
        raise BatchAdaptiveReplanningError(
            "E5 needs its workloads; a run over an empty corpus tests no "
            "shape anybody chose",
            data={"workloads": list(workload_names)})
    if set(scripts) != set(SCRIPTS):
        raise BatchAdaptiveReplanningError(
            "§11 E5 names FOUR reveal scripts and the manifest must carry "
            "all of them; a run that drops one is a different experiment",
            data={"scripts": list(scripts), "required": list(SCRIPTS)})
    return E5Manifest(
        workloads=tuple(workload_names),
        reveal_scripts=tuple(scripts),
        design_named_reveal_scripts=SCRIPTS,
        cases=tuple(cases),
        policies=tuple(policies),
        candidates=tuple(name for name, _ in CANDIDATES),
        roster_version=REPLAN_ROSTER_VERSION,
        comparison_binding_fields=COMPARISON_BINDING_FIELDS,
        epoch_semantics={
            "closure": (
                "§7: an execution result that reveals obligations or "
                "invalidates an estimate CLOSES the epoch. Every event in "
                "every sealed timeline does one of those two things, so "
                "every event closes an epoch"
            ),
            "admission_stop": (
                "on closure the live plan's digest is STALE and every "
                "not-yet-started action of that plan is refused admission "
                "by AdmissionLedger, which RAISES. All three policies go "
                "through the same ledger and none can express the "
                "alternative"
            ),
            "carry": (
                "an action already RUNNING at the closing tick is carried "
                "into the next instance as `carry:<origin>` with its "
                "REMAINING duration, its demand, its declared effects and "
                "revelation, and no prerequisites — the ones it had "
                "already succeeded. It covers everything the origin "
                "covered PLUS a synthetic `must` obligation, which is what "
                "makes the reservation FIXED in a way the INDEPENDENT "
                "CHECKER enforces: §8 refuses any plan that defers a known "
                "`must`. See deferred.fixed_reservation_start_time_not_"
                "expressible for the one part that is not expressible"
            ),
            "observation_binding": (
                "§7: 'bind their eventual observations'. The completed "
                "actions with their completion ticks, the carried "
                "reservations with their remaining durations, and the "
                "fired event indices are digested and that digest is "
                "carried in instance_id, which is inside the instance "
                "digest. Two epochs that inherited different observations "
                "cannot collide"
            ),
            "remaining_batch": (
                "§7: 'replan the REMAINING batch'. A completed action and "
                "every coverage unit it discharged leave the next "
                "instance, and prerequisites on completed work are "
                "dropped because that work SUCCEEDED. Coverage credit "
                "therefore falls between epochs by design, which is what "
                "objective_change_by_epoch reports"
            ),
            "g5": (
                "§7: 'G5 never crosses epochs.' No optimality artifact is "
                "built, no oracle arm runs, and the word optimal is not in "
                "this module's vocabulary"
            ),
        },
        disclosure_ownership=dict(DISCLOSURE_OWNERSHIP),
        publication_rule_strengthening=PUBLICATION_RULE_STRENGTHENING,
        metric_definitions={
            "publication_qualification": (
                "an epoch publishes when the INDEPENDENT CHECKER's verdict "
                "is checked_feasible with no findings AND the epoch is "
                "FINAL — it closed on no revelation and ran to its "
                "plan's makespan. An epoch that closed on a revelation has "
                "not finished; §7 says such a result closes the epoch and "
                "sends it to a replan, and publishing from it would be "
                "publication after a revealable `must`. The FINAL conjunct "
                "makes this rule stricter than §11 E5's sentence read "
                "literally, which is a DEVIATION and not a definition: see "
                "publication_rule_strengthening, which states it and its §7 "
                "justification, and the report's counterfactual census, "
                "which measures what it withholds"
            ),
            "replans": (
                "planning invocations after the first. The first plan of a "
                "run is a PLAN, not a REPLAN, and counting it as one would "
                "charge every policy for existing"
            ),
            "replan_latency": (
                "wall-clock nanoseconds around each planning invocation, "
                "from an INJECTED clock, reported in "
                "E5Report.replan_latency_ns and NOT digested. UNKNOWN with "
                "a reason when no clock is injected — never a zero. See "
                "deferred.replan_latency_wall_time_not_digested"
            ),
            "planning_overhead": (
                "planning_units: the number of actions each planning "
                "invocation had to consider, summed over invocations. This "
                "is the DETERMINISTIC surrogate for replan latency and it "
                "IS digested. Converted to the declared cost unit at "
                "PLANNING_COST_PER_ACTION for the dominance clause"
            ),
            "useful_work": (
                "executed pinned cost that no fired event invalidated: "
                "executed_cost minus wasted_cost. 'Still valid at timeline "
                "end' is decided by the SEALED TIMELINE's own invalidates "
                "sets, not by this harness's opinion"
            ),
            "wasted_work": (
                "pinned cost of actions that had already COMPLETED when an "
                "event naming them in its invalidates set fired. Charged "
                "once per action, at the cost pinned when it ran. An action "
                "that was still RUNNING at that moment is neither wasted "
                "nor saved: §7 carries it as a fixed reservation and binds "
                "its eventual observation, so it finishes and its work "
                "stands. Wasted, running, and saved therefore partition the "
                "invalidated set by what the action was DOING when the "
                "epoch closed, which is the only thing that distinguishes "
                "them"
            ),
            "saved_execution": (
                "pinned cost of old-plan actions that had NOT STARTED when "
                "the epoch closed, that the admission stop then refused, "
                "and that the sealed timeline goes on to invalidate. "
                "Counted once per action across the whole run: this is "
                "exactly what replanning bought. An action already running "
                "was not saved — see wasted_work for the partition"
            ),
            "objective_change_by_epoch": (
                "the §8 tuple as RECOMPUTED by the checker, per epoch. The "
                "planner's claimed value is never the metric. Not summed "
                "across epochs — see deferred.carried_reservation_cost_"
                "charged_once_at_completion"
            ),
            "delayed_mandatory_work": (
                "idle ticks between an invalidation and the replan that "
                "follows it, counted only while a `must` obligation is "
                "still undischarged. Only the staged-barrier policy can "
                "accrue any: static and rolling replan at the closing "
                "tick, so their idle window is zero by construction, and "
                "reporting a zero for them is a structural fact rather "
                "than a measurement"
            ),
            "may_change_precision_recall": (
                "per qualified publication: the claimed MayChange set "
                "against EVENTUAL FULL PROCESSING of the sealed timeline, "
                "restricted to the actions that epoch did not complete, "
                "over harness-only truth_locations. Reported UNREDUCED as "
                "'hit/total'. Recall threshold ONE; precision has no "
                "threshold and is the cost of conservatism"
            ),
            "admission_stop": (
                "not-yet-started actions of an invalidated plan that were "
                "ATTEMPTED against the live ledger and refused. The "
                "attempt is real: the clause reads what happened, not what "
                "would have"
            ),
        },
        secondary_metrics={
            "disclosure_grading_all_checked_epochs": (
                "the same MayChange grade over EVERY checked-feasible "
                "epoch, published or not. Reported because a mid-timeline "
                "disclosure is still evidence about the widening rules, "
                "and it decides NOTHING: it does not enter the "
                "zero-tolerance clause, the verdict, or the cost "
                "conclusion, because a mid-timeline epoch is not a "
                "publication surface"
            ),
            "carried_reservation_placed_at_zero": (
                "how often a plan placed a carried reservation at start "
                "zero. The frozen schema cannot REQUIRE it (see "
                "deferred.fixed_reservation_start_time_not_expressible), "
                "so this is reported and decides nothing; a planner that "
                "placed one later produced a plan the checker still "
                "accepted"
            ),
            "break_even_planning_cost_per_action": (
                "the largest PLANNING_COST_PER_ACTION at which total "
                "planning cost would still not exceed total saved "
                "execution. Reported beside the dominance verdict so the "
                "declared conversion rate's influence is visible instead "
                "of implicit. Decides nothing"
            ),
            "policy_totals": (
                "wasted, useful, saved, planning, delayed-mandatory, and "
                "replan totals per policy. Reported so the three policies "
                "can be read against one another; the zero-tolerance "
                "clauses bind all three identically and these totals enter "
                "none of them"
            ),
        },
        cost_unit=COST_UNIT,
        time_unit=TIME_UNIT,
        planning_cost_per_action=PLANNING_COST_PER_ACTION,
        max_epochs=MAX_EPOCHS,
        minimum_admission_stop_probes=MINIMUM_ADMISSION_STOP_PROBES,
        minimum_graded_publications=MINIMUM_GRADED_PUBLICATIONS,
        minimum_cost_accounted_runs=MINIMUM_COST_ACCOUNTED_RUNS,
        zero_tolerance={name: CLAUSES[name] for name in ZERO_TOLERANCE},
        falsification_clauses=dict(CLAUSES),
        primary_metric=(
            "net replanning benefit in the instance's single declared cost "
            "unit: total saved execution minus total planning cost, summed "
            "across every accounted run. Positive means replanning saved "
            "more work than it cost; zero or negative fires the "
            "replanning_cost_dominates_saved_execution clause"
        ),
        comparator=(
            "ZERO. §11 E5's clause is a DOMINANCE test — 'replanning cost "
            "dominates saved execution across the tested workloads' — not "
            "a comparison against another arm, so the comparator is the "
            "break-even point and not a baseline policy. The three "
            "policies are compared to one another on the SECONDARY policy "
            "totals, which decide nothing"
        ),
        aggregation=(
            "exact sums over the whole grid: every workload x reveal "
            "script x case x policy x candidate. The corpus is "
            "deterministic and the candidate roster is exhaustive per "
            "instance, so there is no sampling to aggregate over"
        ),
        uncertainty=(
            "deterministic exhaustive counts — no sampling, no confidence "
            "interval. Fractions are reported UNREDUCED so the evidence "
            "behind them is visible. A run that refused is counted in the "
            "refusal census and excluded from the denominators it could "
            "not contribute to, never silently averaged in as a zero"
        ),
        threshold=(
            "every zero-tolerance clause at zero, over denominators of at "
            f"least {MINIMUM_ADMISSION_STOP_PROBES} admission-stop probes "
            f"and {MINIMUM_GRADED_PUBLICATIONS} graded publications; and — "
            "for the replanning-pays conclusion specifically — total saved "
            "execution at least equal to total planning cost over a "
            f"denominator of at least {MINIMUM_COST_ACCOUNTED_RUNS} "
            "accounted runs. Beneath any floor the report concludes "
            f"'{CONCLUSION_NOT_ESTABLISHED}' and the verdict says invalid, "
            "rather than publishing a negative sentence over a measurement "
            "nobody made"
        ),
        deferred=dict(DEFERRED),
    )


#: The PREREGISTERED manifest digest: the digest of ``build_manifest()``
#: under its default configuration, pinned so that an outcome-driven edit
#: to the corpus, the grid, the clauses, the metric definitions, the
#: comparator, the thresholds, or the deferrals cannot pass silently —
#: changing the configuration means changing this constant in the same
#: reviewable diff. A named test asserts the equality.
PREREGISTERED_MANIFEST_DIGEST = (
    "7a1534334f5c1e02c0b90295e8d28bc6a0ff48814a02cec44268db818913fbaf"
)


# --------------------------------------------------------------------------- #
# The cost account and its derived conclusion.                                 #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CostAccount:
    """The replanning-cost comparison §11 E5's fourth clause is about.

    Everything here is an exact integer sum in the instance's SINGLE
    declared cost unit. There is no averaging, so there is nothing for a
    refusing run to be silently averaged into: a run that never reached a
    cost account is in the refusal census and out of ``accounted``."""

    accounted: int
    minimum_accounted: int
    planning_units: int
    planning_cost: int
    saved_execution: int
    executed_cost: int
    wasted_cost: int
    useful_cost: int
    per_policy: tuple[Mapping[str, Any], ...]
    not_comparable: tuple[str, ...]

    @property
    def established(self) -> bool:
        """Enough accounted runs to say anything at all."""
        return self.accounted >= self.minimum_accounted > 0

    @property
    def net_benefit(self) -> int:
        """The primary metric: saved execution minus planning cost."""
        return self.saved_execution - self.planning_cost

    @property
    def dominates(self) -> bool:
        """Does planning cost EXCEED saved execution? The clause's test."""
        return self.planning_cost > self.saved_execution

    @property
    def break_even_planning_cost_per_action(self) -> int:
        """The largest per-action rate at which planning would not dominate.

        Reported so the DECLARED conversion rate's influence on the
        dominance answer is visible rather than implicit. Zero means no
        positive rate avoids dominance; it is not a missing measurement,
        and a zero planning-unit total reports the rate as not applicable
        by returning zero with the denominator visible beside it."""
        if self.planning_units <= 0:
            return 0
        return self.saved_execution // self.planning_units

    @property
    def break_even_rate_exact(self) -> float:
        """The EXACT rate at which planning cost would equal saved execution.

        :attr:`break_even_planning_cost_per_action` divides with an integer
        floor, so it reads ZERO whenever saved execution is smaller than the
        planning-unit count — and a zero there was being reported as "every
        positive rate gives the same sign", which holds only over the INTEGERS
        (audit, 2026-08-29). The conversion rate is a modelling quantity with
        no reason to be whole: planning here is arithmetic over a handful of
        actions while one action is a document extraction, so the plausible
        rate is well below 1 and the range the integer floors away is exactly
        the range that matters. Zero planning units reports 0.0 with the
        denominator visible beside it, as the integer form does.
        """
        if self.planning_units <= 0:
            return 0.0
        return self.saved_execution / self.planning_units

    @property
    def saved_per_planning_unit(self) -> str:
        """Saved execution per planning unit, UNREDUCED.

        The integer break-even rate floors to zero as soon as saved
        execution is smaller than the planning-unit count, and a bare
        zero hides how far from break-even the answer actually is. This
        fraction carries both numbers."""
        return f"{self.saved_execution}/{self.planning_units}"

    @property
    def rate_sensitivity(self) -> str:
        """How far the dominance answer sits from flipping on the rate.

        The conclusion token names the declared conversion rate as a
        premise; this says what the premise is worth. It is DERIVED from
        the break-even rate, never authored, and it is the sentence a
        reader would otherwise have to infer from a bare zero."""
        if not self.established:
            return (
                "NOT EVALUATED: no dominance answer was published, so "
                "there is no rate sensitivity to state")
        if not self.dominates:
            return (
                f"replanning pays at the declared rate of "
                f"{PLANNING_COST_PER_ACTION} per action; it would stop "
                f"paying above a rate of {self.break_even_rate_exact:.4f} "
                f"({self.saved_per_planning_unit}), which the integer "
                f"break-even reports as "
                f"{self.break_even_planning_cost_per_action}")
        if self.break_even_planning_cost_per_action <= 0:
            return (
                f"no INTEGER rate of 1 or more avoids dominance: the integer "
                f"break-even floors to zero. That is NOT rate-independence, "
                f"and this sentence used to claim it was (audit, 2026-08-29). "
                f"Planning cost equals saved execution at a rate of "
                f"{self.break_even_rate_exact:.4f} "
                f"({self.saved_per_planning_unit}), and at any declared rate "
                f"BELOW that the sign reverses and replanning pays. The rate "
                f"premise is load-bearing, and the plausible range is the one "
                f"the integer floor hides: planning is arithmetic over a "
                f"handful of actions while one action is a document "
                f"extraction")
        return (
            f"the dominance verdict DOES depend on the declared rate: "
            f"break-even is {self.break_even_planning_cost_per_action}, so "
            f"any rate at or below it would not dominate, and the declared "
            f"{PLANNING_COST_PER_ACTION} is above it")

    @property
    def conclusion(self) -> str:
        if not self.established:
            return CONCLUSION_NOT_ESTABLISHED
        if self.dominates:
            return CONCLUSION_REPLANNING_DOES_NOT_PAY
        return CONCLUSION_REPLANNING_PAYS

    @property
    def headline(self) -> str:
        if not self.established:
            return (
                f"NOT EVALUATED: {self.accounted} accounted run(s) against a "
                f"preregistered minimum of {self.minimum_accounted}")
        return (
            f"planning cost {self.planning_cost} versus saved execution "
            f"{self.saved_execution} over {self.accounted} accounted run(s) "
            f"({self.planning_units} planning units at a declared "
            f"{PLANNING_COST_PER_ACTION} per action; break-even rate "
            f"{self.break_even_rate_exact:.4f}, which the integer form floors "
            f"to {self.break_even_planning_cost_per_action}; unreduced "
            f"saved-per-planning-unit {self.saved_per_planning_unit}). "
            f"Executed "
            f"{self.executed_cost}, of which {self.wasted_cost} was "
            f"invalidated and {self.useful_cost} stood")

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "accounted": self.accounted,
            "minimum_accounted": self.minimum_accounted,
            "established": self.established,
            "planning_units": self.planning_units,
            "planning_cost": self.planning_cost,
            "planning_cost_per_action": PLANNING_COST_PER_ACTION,
            "break_even_planning_cost_per_action": (
                self.break_even_planning_cost_per_action),
            # A DECIMAL STRING, never a float: the digest dialect refuses
            # floats (a float in digested content is a digest that moves
            # between machines), and that guard is right. The EXACT value stays
            # the integer pair in `saved_per_planning_unit` beside this; this
            # key is the rounded rendering a reader can compare a declared rate
            # against without dividing it themselves.
            "break_even_rate_decimal": f"{self.break_even_rate_exact:.6f}",
            "saved_per_planning_unit": self.saved_per_planning_unit,
            "saved_execution": self.saved_execution,
            "net_benefit": self.net_benefit,
            "executed_cost": self.executed_cost,
            "wasted_cost": self.wasted_cost,
            "useful_cost": self.useful_cost,
            "dominates": self.dominates,
            "conclusion": self.conclusion,
            "rate_sensitivity": self.rate_sensitivity,
            "headline": self.headline,
            "per_policy": [dict(sorted(p.items())) for p in self.per_policy],
            "not_comparable": list(self.not_comparable),
        }


def cost_conclusion(
    runs: Sequence[PolicyRun], *, minimum_accounted: int,
) -> CostAccount:
    """Sum the cost account over the runs that reached one.

    A run is ACCOUNTED when it produced at least one checked epoch. A run
    the planner or the checker refused contributed no execution and no
    saved execution, and including it would let a refusal read as a
    perfectly balanced account.

    Every accounted run is checked against
    :data:`COMPARISON_BINDING_FIELDS` before its numbers join the sums.
    That is not a per-run sanity check — those cannot see a cross-run
    mismatch — it is a guard that two runs whose totals are added really
    do come from one experiment: same manifest, same roster, same
    candidate identity, and a workload and timeline digest each run
    carries for itself."""
    accounted: list[PolicyRun] = []
    not_comparable: list[str] = []
    for run in runs:
        key = f"{run.workload}/{run.script}/{run.case}/{run.policy}/{run.candidate}"
        if not run.epochs:
            not_comparable.append(f"{key}:no_epochs")
            continue
        if not any(e.status == STATUS_CHECKED_FEASIBLE for e in run.epochs):
            not_comparable.append(f"{key}:no_checked_epoch")
            continue
        missing = [
            field for field in COMPARISON_BINDING_FIELDS
            if not str(getattr(run, field, "") or "")
        ]
        if missing:
            not_comparable.append(f"{key}:unbound:{','.join(sorted(missing))}")
            continue
        accounted.append(run)

    per_policy: list[Mapping[str, Any]] = []
    for policy in POLICIES:
        rows = [r for r in accounted if r.policy == policy]
        # The structural-zero label, INLINE on the row it qualifies.
        # Only the staged-barrier policy has an idle window between an
        # invalidation and the replan that follows it; static and rolling
        # replan at the closing tick, so their zero is a fact about the
        # policy and not a measurement of it. Read as a measurement, the
        # comparison says staged loses to two policies that cannot accrue
        # the metric at all, which is not a comparison.
        delayed_is_structural = policy != POLICY_STAGED_BARRIERS
        per_policy.append({
            "delayed_mandatory_ticks_zero_by_construction": (
                delayed_is_structural),
            "delayed_mandatory_ticks_label": (
                "ZERO BY CONSTRUCTION: this policy replans at the closing "
                "tick, so it has no idle window in which mandatory work "
                "could be delayed. A zero here is a structural fact and "
                "NOT a measured advantage over a policy that can accrue "
                "the metric"
                if delayed_is_structural
                else "MEASURED: this policy defers its replan to a "
                     "barrier, so the idle ticks between an invalidation "
                     "and that replan are a real quantity"),
            "policy": policy,
            "runs": len(rows),
            "replans": sum(r.replans for r in rows),
            "planning_units": sum(r.planning_units for r in rows),
            "planning_cost": sum(r.planning_cost for r in rows),
            "saved_execution": sum(r.saved_execution for r in rows),
            "executed_cost": sum(r.executed_cost for r in rows),
            "wasted_cost": sum(r.wasted_cost for r in rows),
            "useful_cost": sum(r.useful_cost for r in rows),
            "delayed_mandatory_ticks": sum(
                r.delayed_mandatory_ticks for r in rows),
            "qualified_publications": sum(
                len(r.qualified_publications) for r in rows),
        })

    return CostAccount(
        accounted=len(accounted),
        minimum_accounted=minimum_accounted,
        planning_units=sum(r.planning_units for r in accounted),
        planning_cost=sum(r.planning_cost for r in accounted),
        saved_execution=sum(r.saved_execution for r in accounted),
        executed_cost=sum(r.executed_cost for r in accounted),
        wasted_cost=sum(r.wasted_cost for r in accounted),
        useful_cost=sum(r.useful_cost for r in accounted),
        per_policy=tuple(per_policy),
        not_comparable=tuple(sorted(not_comparable)),
    )


# --------------------------------------------------------------------------- #
# The non-vacuity witnesses. A clause whose zero is structural is not a        #
# result, so each central clause has a probe that MUST fire.                   #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class AdmissionStopWitness:
    """A directed proof that the admission stop actually refuses.

    The zero on :data:`CLAUSE_STALE_PLAN_CONTINUES_ADMISSION` is only
    evidence if the stop can bite. This probe opens an epoch, invalidates
    it, and attempts an admission that had not started — and the report is
    INVALID if the attempt was not refused, because a stop that stopped
    stopping would produce the same clean zero as a stop that works."""

    refused: bool
    code: str
    detail: str

    def to_canonical_dict(self) -> dict[str, Any]:
        return {"refused": self.refused, "code": self.code,
                "detail": self.detail}


def admission_stop_witness() -> AdmissionStopWitness:
    ledger = AdmissionLedger()
    ledger.open_epoch(epoch=0, instance_digest="witness-instance",
                      plan_digest="witness-plan")
    ledger.admit("already_running", plan_digest="witness-plan", at=0)
    ledger.invalidate(at=1, reason="witness invalidation")
    try:
        ledger.admit("not_yet_started", plan_digest="witness-plan", at=2)
    except StalePlanAdmission as exc:
        return AdmissionStopWitness(
            refused=True, code=str(exc.data.get("code", "")),
            detail=("an action that had not started was refused admission "
                    "after the epoch closed, so the stop can bite"))
    return AdmissionStopWitness(
        refused=False, code="",
        detail=("THE ADMISSION STOP DID NOT REFUSE. Every zero on the "
                "stale-admission clause is vacuous while this is so"))


@dataclass(frozen=True)
class QualificationWitness:
    """A directed proof that publication is deferred and then RESUMES.

    §11 E5 asks for the refusal AND for qualified publication resuming
    after replanning covers the revealed work. Derived from the grid, not
    hand-built: the witness looks for a run that deferred publication at
    an epoch with a pending mandatory revelation and then published a
    qualified epoch later in the SAME run."""

    fired: bool
    run_key: str
    deferred_epoch: int
    qualified_epoch: int
    checker_refusal_causes: tuple[str, ...]
    detail: str

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "fired": self.fired,
            "run_key": self.run_key,
            "deferred_epoch": self.deferred_epoch,
            "qualified_epoch": self.qualified_epoch,
            "checker_refusal_causes": list(self.checker_refusal_causes),
            "detail": self.detail,
        }


def _qualification_witness(runs: Sequence[PolicyRun]) -> QualificationWitness:
    causes: set[str] = set()
    for run in runs:
        for row in run.epochs:
            causes.update(c for c in row.causes
                          if c in QUALIFICATION_REFUSAL_CAUSES)
    for run in sorted(
        runs, key=lambda r: (r.workload, r.script, r.case, r.policy,
                             r.candidate),
    ):
        deferred_at = None
        for row in run.epochs:
            if (deferred_at is None
                    and not row.publication_qualified
                    and row.publication_deferred_reason ==
                    DEFERRED_EPOCH_CLOSED_ON_INVALIDATION
                    and row.pending_must_revealable):
                deferred_at = row.epoch
                continue
            if deferred_at is not None and row.publication_qualified:
                return QualificationWitness(
                    fired=True,
                    run_key=(f"{run.workload}/{run.script}/{run.case}/"
                             f"{run.policy}/{run.candidate}"),
                    deferred_epoch=deferred_at,
                    qualified_epoch=row.epoch,
                    checker_refusal_causes=tuple(sorted(causes)),
                    detail=(
                        "publication was DEFERRED at the epoch that revealed "
                        "a mandatory obligation and RESUMED, qualified, once "
                        "a later epoch's replan covered it. The checker's own "
                        "mandatory and revelation refusals are observed "
                        "elsewhere in the grid and are listed beside this"),
                )
    return QualificationWitness(
        fired=False, run_key="", deferred_epoch=-1, qualified_epoch=-1,
        checker_refusal_causes=tuple(sorted(causes)),
        detail=("NO RUN DEFERRED PUBLICATION FOR A REVEALED MANDATORY "
                "OBLIGATION AND THEN QUALIFIED. The late-must and cascade "
                "scripts are not exercising what they are named for"))


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
class E5Report:
    """The whole E5 replay. ``verdict`` is DERIVED, never authored.

    :attr:`replan_latency_ns` is the ONE field
    :meth:`to_canonical_dict` does not emit. §11 E5's metric list names
    replan latency, so the harness will measure it when a clock is
    injected; a wall-clock reading inside digested content would be a
    digest that moves between machines, so it stays outside."""

    manifest_digest: str
    workloads: tuple[Mapping[str, Any], ...]
    timelines: tuple[Mapping[str, Any], ...]
    runs: tuple[PolicyRun, ...]
    cost: CostAccount
    admission_stop: AdmissionStopWitness
    qualification: QualificationWitness
    disclosure_published: Mapping[str, Any]
    disclosure_all_checked: Mapping[str, Any]
    falsifications: tuple[FalsificationRow, ...]
    deferred: Mapping[str, str]
    #: Carried from the manifest and EMITTED, because a label that never
    #: reaches the report cannot make a number honest for a reader. The
    #: structural-zero disclaimer on delayed mandatory work and the
    #: "decides nothing" disclaimers on the secondary surfaces both live
    #: here, and the rows they qualify are in this same document.
    metric_definitions: Mapping[str, str] = field(default_factory=dict)
    secondary_metrics: Mapping[str, str] = field(default_factory=dict)
    publication_rule_strengthening: str = ""
    #: NOT digested. Empty when no clock was injected, which is the
    #: default, and which the manifest calls UNKNOWN rather than zero.
    replan_latency_ns: tuple[int, ...] = ()

    def fired(self) -> tuple[str, ...]:
        return tuple(f.clause for f in self.falsifications if f.fired)

    def replan_latency(self) -> Mapping[str, Any]:
        """Replan latency, or the named reason there is none."""
        if not self.replan_latency_ns:
            return {"status": "unknown", "reason": (
                "no clock was injected, so replan latency was not measured; "
                "an unmeasured quantity is UNKNOWN, never a zero")}
        return {
            "status": "measured",
            "samples": len(self.replan_latency_ns),
            "total_ns": sum(self.replan_latency_ns),
            "max_ns": max(self.replan_latency_ns),
        }

    def refusal_census(self) -> Mapping[str, int]:
        """Every run that did NOT reach the checker's feasible verdict.

        A refusing run shrinks every denominator downstream of it, and the
        only trace it would otherwise leave is a smaller number. This
        census is a top-level report field and the verdict names its
        total."""
        census: dict[str, int] = {}
        for run in self.runs:
            if run.status == STATUS_CHECKED_FEASIBLE:
                continue
            for key in (f"status:{run.status}", f"policy:{run.policy}",
                        f"script:{run.script}", f"case:{run.case}",
                        f"workload:{run.workload}",
                        f"candidate:{run.candidate}"):
                census[key] = census.get(key, 0) + 1
        census["total"] = sum(
            1 for r in self.runs if r.status != STATUS_CHECKED_FEASIBLE)
        return census

    def publication_reason_census(self) -> Mapping[str, int]:
        """Which branch decided each epoch's publication, and how often.

        Every reason in the closed vocabulary is emitted, INCLUDING the
        ones that decided nothing: a branch that never fires is a fact a
        reader should be able to read off the report rather than
        reverse-engineer from a missing key. ``qualified`` is the empty
        reason, named here so the histogram sums to the epoch count."""
        census = {
            "qualified": 0,
            DEFERRED_CHECKER_REFUSED: 0,
            DEFERRED_EPOCH_CLOSED_ON_INVALIDATION: 0,
            DEFERRED_POLICY_BARRIER: 0,
            STATUS_PLANNER_REFUSED: 0,
        }
        for run in self.runs:
            for row in run.epochs:
                key = (row.publication_deferred_reason
                       or "qualified")
                census[key] = census.get(key, 0) + 1
        census["total"] = sum(len(r.epochs) for r in self.runs)
        return census

    def literal_publication_counterfactual(
        self,
    ) -> Mapping[str, Mapping[str, int]]:
        """What §11 E5's LITERAL clause would have published per branch.

        A top-level report surface, because the strengthening declared in
        :attr:`publication_rule_strengthening` is only credible if a
        reader can weigh what it withholds. Secondary: it decides no
        clause and no verdict."""
        return literal_publication_counterfactual(self.runs)

    @property
    def verdict(self) -> str:
        return self._verdict_stem() + self._refusal_suffix()

    def _refusal_suffix(self) -> str:
        refused = self.refusal_census()["total"]
        if not refused:
            return ""
        return f":{VERDICT_REFUSALS_SUFFIX}:{refused}"

    def _verdict_stem(self) -> str:
        if not self.runs:
            return "invalid:no_runs"
        if not any(r.status == STATUS_CHECKED_FEASIBLE for r in self.runs):
            return "invalid:no_checked_feasible_run"
        if not self.admission_stop.refused:
            return "invalid:admission_stop_witness_did_not_fire"
        if not self.qualification.fired:
            return "invalid:qualification_witness_did_not_fire"
        probes = int(self.disclosure_published["admission_stop_probes"])
        if probes < MINIMUM_ADMISSION_STOP_PROBES:
            return "invalid:admission_stop_denominator_below_minimum"
        graded = int(self.disclosure_published["graded_with_reference"])
        if graded < MINIMUM_GRADED_PUBLICATIONS:
            return (
                "invalid:graded_reference_publication_denominator_"
                "below_minimum")
        for row in self.falsifications:
            if row.fired and row.zero_tolerance:
                return f"refuted:{row.clause}"
        if not self.cost.established:
            return "invalid:cost_denominator_below_minimum"
        for row in self.falsifications:
            if row.fired:
                return f"replanning_claim_rejected:{row.clause}"
        partial = tuple(
            name for name in PARTIAL_E5_DEFERRALS if name in self.deferred)
        if not partial:
            return VERDICT_PASS
        return f"{VERDICT_PASS_PARTIAL_E5}:{','.join(partial)}"

    def cost_conclusion(self) -> str:
        """The cost conclusion, or a refusal to publish one."""
        if self._verdict_stem().startswith("invalid:"):
            if not self.cost.established:
                return CONCLUSION_NOT_ESTABLISHED
            return CONCLUSION_NOT_ESTABLISHED_INVALID
        return self.cost.conclusion

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "schema": "okg.synth.e5-adaptive-replanning-report.v1",
            "manifest_digest": self.manifest_digest,
            "verdict": self.verdict,
            "cost_conclusion": self.cost_conclusion(),
            "refusal_census": dict(sorted(self.refusal_census().items())),
            "publication_reason_census": dict(
                sorted(self.publication_reason_census().items())),
            # The declared deviation and its measured cost, in the
            # document that carries the clause it deviates from. A
            # strengthening a reader cannot see is indistinguishable from
            # a strengthening nobody declared.
            "publication_rule_strengthening": (
                self.publication_rule_strengthening),
            "literal_publication_counterfactual": {
                reason: dict(row) for reason, row
                in self.literal_publication_counterfactual().items()},
            # The LABELS, in the report rather than only in the manifest.
            # A structural zero that travels without its disclaimer reads
            # as a measured zero, and the per-policy comparison below
            # carries the same label inline on every row it qualifies.
            "metric_definitions": dict(sorted(self.metric_definitions.items())),
            "secondary_metrics": dict(sorted(self.secondary_metrics.items())),
            # A CONSTANT pointer, never a reading. §11 E5 names replan
            # latency and the report must disclose where it lives, but a
            # wall-clock number here — even a summary of one — would be a
            # digest that moves between machines and between runs on the
            # same one. The readings are in replan_latency_ns and
            # replan_latency(), both outside the digest.
            "replan_latency": (
                "NOT DIGESTED: see E5Report.replan_latency() and "
                "E5Report.replan_latency_ns, and "
                "deferred.replan_latency_wall_time_not_digested"),
            "checker_version": CHECKER_VERSION,
            "producer_version": PRODUCER_VERSION,
            "disclosure_scope_digest_algorithm": SCOPE_DIGEST_ALGORITHM,
            "disclosure_scope_id_limit": SCOPE_ID_LIMIT,
            "workloads": [dict(sorted(w.items())) for w in self.workloads],
            "timelines": [dict(sorted(t.items())) for t in self.timelines],
            "runs": [r.to_canonical_dict() for r in self.runs],
            "cost": self.cost.to_canonical_dict(),
            "admission_stop_witness": self.admission_stop.to_canonical_dict(),
            "qualification_witness": self.qualification.to_canonical_dict(),
            "disclosure_published": dict(
                sorted(self.disclosure_published.items())),
            "disclosure_all_checked": dict(
                sorted(self.disclosure_all_checked.items())),
            "falsifications": [
                f.to_canonical_dict() for f in self.falsifications],
            "deferred": dict(sorted(self.deferred.items())),
        }

    def canonical_json(self) -> str:
        return _local_canonical_json(self.to_canonical_dict())

    def digest(self) -> str:
        return _local_digest(self.to_canonical_dict())


def _disclosure_summary(
    runs: Sequence[PolicyRun], *, published_only: bool,
) -> Mapping[str, Any]:
    """Aggregate the MayChange grades, unreduced and refusal-aware.

    The graded rows are SPLIT by whether the epoch had anything to be
    wrong about. A row whose ``recall_denominator`` is zero — eventual
    full processing changes nothing that epoch left undone — is contained
    VACUOUSLY: no claim, however narrow, could have missed a location
    there. Summing those rows into one ``graded`` count and then weighing
    the zero-tolerance recall against it states the floor over a quantity
    most of which decides nothing. So ``graded_with_reference`` is the
    denominator the recall clause and :data:`MINIMUM_GRADED_PUBLICATIONS`
    are stated over, ``graded_without_reference`` is reported beside it as
    a named field rather than left to be reconstructed row by row, and
    ``graded`` remains their sum for continuity."""
    graded = 0
    graded_with_reference = 0
    graded_without_reference = 0
    contained = 0
    recall_hit = 0
    recall_total = 0
    precision_hit = 0
    precision_total = 0
    misses: list[str] = []
    probes = 0
    refusals = 0
    escaped: list[str] = []
    all_locations: set[str] = set()
    for run in runs:
        for row in run.epochs:
            probes += row.admission_stop_probes
            refusals += row.admission_stop_refusals
            escaped.extend(row.admitted_after_stale)
            if row.grade is None:
                continue
            if published_only and not row.publication_qualified:
                continue
            if not published_only and row.status != STATUS_CHECKED_FEASIBLE:
                continue
            graded += 1
            if row.grade.recall_denominator > 0:
                graded_with_reference += 1
            else:
                graded_without_reference += 1
            all_locations.update(row.grade.claimed)
            recall_hit += row.grade.recall_numerator
            recall_total += row.grade.recall_denominator
            precision_hit += row.grade.precision_numerator
            precision_total += row.grade.precision_denominator
            if row.grade.contained:
                contained += 1
            else:
                misses.append(
                    f"{run.workload}/{run.script}/{run.case}/{run.policy}/"
                    f"{run.candidate}@{row.epoch}:"
                    f"{','.join(row.grade.missing)}")
    return {
        "graded": graded,
        "graded_with_reference": graded_with_reference,
        "graded_without_reference": graded_without_reference,
        "contained": contained,
        "recall": f"{recall_hit}/{recall_total}",
        "precision": f"{precision_hit}/{precision_total}",
        "misses": sorted(misses),
        "admission_stop_probes": probes,
        "admission_stop_refusals": refusals,
        "admitted_after_stale": sorted(escaped),
        "identity": dict(sorted(
            disclosure_identity(sorted(all_locations)).items())),
    }


def literal_publication_counterfactual(
    runs: Sequence[PolicyRun],
) -> Mapping[str, Mapping[str, int]]:
    """What §11 E5's LITERAL clause would have published, and at what cost.

    :data:`PUBLICATION_RULE_STRENGTHENING` records that this lane's
    publication rule is STRICTER than §11 E5's sentence read literally.
    A claim of that shape is only worth anything if a reader can see how
    much the strictness is doing, so this census computes it rather than
    asserting it.

    For every epoch that did NOT publish, the LITERAL reading — the
    checker accepts, reports no cause, no `must` is known uncovered and
    none is still revealable — is applied to decide whether §11 E5's own
    sentence would have deferred that epoch. ``would_publish`` counts the
    epochs it would NOT have deferred; ``would_publish_with_disclosure_
    miss`` counts how many of those carry a MayChange set that eventual
    full processing escapes. The second number is the load-bearing one:
    it is the disclosure damage the stricter rule prevents, and it is
    exactly the evidence that the strengthening is not decoration.

    Every branch of the closed deferral vocabulary is emitted, including
    the ones that count zero, so a branch that stopped mattering is
    readable rather than a missing key."""
    census: dict[str, dict[str, int]] = {
        reason: {"rows": 0, "would_publish": 0,
                 "would_publish_with_disclosure_miss": 0}
        for reason in (DEFERRED_CHECKER_REFUSED,
                       DEFERRED_EPOCH_CLOSED_ON_INVALIDATION,
                       DEFERRED_POLICY_BARRIER,
                       STATUS_PLANNER_REFUSED)
    }
    for run in runs:
        for row in run.epochs:
            if row.publication_qualified:
                continue
            entry = census.setdefault(
                row.publication_deferred_reason,
                {"rows": 0, "would_publish": 0,
                 "would_publish_with_disclosure_miss": 0})
            entry["rows"] += 1
            literal_defers = (
                row.status != STATUS_CHECKED_FEASIBLE
                or bool(row.causes)
                or bool(row.uncovered_must)
                or bool(row.pending_must_revealable))
            if literal_defers:
                continue
            entry["would_publish"] += 1
            if row.grade is not None and not row.grade.contained:
                entry["would_publish_with_disclosure_miss"] += 1
    return {
        reason: dict(sorted(entry.items()))
        for reason, entry in sorted(census.items())
    }


def run_report(
    manifest: E5Manifest, *, clock: Callable[[], int] | None = None,
    affected_scope_override: Mapping[str, Sequence[str]] | None = None,
) -> E5Report:
    """Run the whole preregistered E5 grid.

    For each workload: build the sealed timeline for every reveal script
    and case, drive it under every admission policy with every candidate,
    and hand every epoch's plan to the independent checker.

    ``affected_scope_override`` exists for the directed under-disclosure
    mutant, keyed by workload name. It is not part of the preregistered
    configuration and it does not enter the manifest digest: a mutant is a
    test's instrument, not an experiment arm, and a run that used one
    reports a different report digest by construction."""
    corpus = {w.name: w for w in workloads()
              if w.name in set(manifest.workloads)}
    missing = sorted(set(manifest.workloads) - set(corpus))
    if missing:
        raise BatchAdaptiveReplanningError(
            "the manifest names a workload the corpus does not build",
            data={"missing": missing})
    manifest_digest = manifest.digest()
    overrides = dict(affected_scope_override or {})

    workload_rows: list[Mapping[str, Any]] = []
    timeline_rows: list[Mapping[str, Any]] = []
    runs: list[PolicyRun] = []
    latencies: list[int] = []
    for name in manifest.workloads:
        workload = corpus[name]
        workload_rows.append({
            "name": workload.name,
            "source": workload.source,
            "exercises": workload.exercises,
            "workload_digest": workload.digest,
            "actions": len(workload.actions),
            "obligations": len(workload.obligations),
            "revealer": workload.revealer,
            "case_producer": workload.case_producer,
            "shadow": workload.shadow,
        })
        for script in manifest.reveal_scripts:
            for case in manifest.cases:
                timeline = build_timeline(workload, script, case)
                timeline_rows.append({
                    "workload": workload.name,
                    "script": script,
                    "case": case,
                    "timeline_digest": timeline.digest,
                    "events": len(timeline.events),
                    "eventual_locations": len(
                        eventual_locations(workload, script, case)),
                })
                for policy in manifest.policies:
                    for candidate in manifest.candidates:
                        run, taken = run_policy(
                            workload, script, case, policy, candidate,
                            manifest_digest=manifest_digest,
                            affected_scope=overrides.get(workload.name),
                            clock=clock)
                        runs.append(run)
                        latencies.extend(taken)

    cost = cost_conclusion(
        runs, minimum_accounted=manifest.minimum_cost_accounted_runs)
    published = _disclosure_summary(runs, published_only=True)
    all_checked = _disclosure_summary(runs, published_only=False)
    stop_witness = admission_stop_witness()
    qualification = _qualification_witness(runs)

    escaped = list(published["admitted_after_stale"])
    read_as_pass = [
        f"{r.workload}/{r.script}/{r.case}/{r.policy}/{r.candidate}@{e.epoch}"
        for r in runs for e in r.epochs
        if e.publication_qualified
        and (e.status != STATUS_CHECKED_FEASIBLE or e.causes)
    ]
    published_with_must = list(
        publications_with_outstanding_must(runs))
    publication_probes = sum(
        1 for r in runs for e in r.epochs
        if e.status in (STATUS_CHECKED_FEASIBLE, STATUS_REFUSED))
    deferred_for_must = sum(
        1 for r in runs for e in r.epochs
        if not e.publication_qualified
        and (e.pending_must_revealable or e.uncovered_must))
    counterfactual = literal_publication_counterfactual(runs)
    barrier_row = counterfactual[DEFERRED_POLICY_BARRIER]
    invalidation_row = counterfactual[DEFERRED_EPOCH_CLOSED_ON_INVALIDATION]
    #: The recall clause's real denominator, and whether it reaches its
    #: preregistered floor. Below the floor the clause REFUSES rather than
    #: reporting a clean zero over rows that could not have been wrong.
    graded_with_reference = int(published["graded_with_reference"])
    disclosure_established = (
        graded_with_reference >= MINIMUM_GRADED_PUBLICATIONS > 0)

    falsifications = (
        FalsificationRow(
            clause=CLAUSE_CHECKER_REFUSAL_READ_AS_PASS,
            fired=bool(read_as_pass),
            zero_tolerance=True,
            evaluated=publication_probes,
            detail=("every reported qualification is the checker's own "
                    "checked_feasible verdict with no findings"
                    if not read_as_pass
                    else "REFUSAL REPORTED AS QUALIFIED: "
                         + ",".join(sorted(read_as_pass))),
        ),
        FalsificationRow(
            clause=CLAUSE_PUBLICATION_AFTER_REVEALABLE_MUST,
            fired=bool(published_with_must),
            zero_tolerance=True,
            evaluated=publication_probes,
            detail=(
                (
                    "no qualified publication carried an uncovered "
                    "mandatory unit (re-derived here from the plan's own "
                    "witnesses) or an unfired timeline event that would "
                    "reveal a mandatory obligation"
                    if not published_with_must
                    else "PUBLISHED WITH A KNOWN OR REVEALABLE MUST "
                         "OUTSTANDING: "
                         + ",".join(sorted(published_with_must))
                )
                + f". {deferred_for_must} epoch(s) DEFERRED publication for "
                "exactly that reason, which is the behaviour the clause "
                "exists to confirm; the checker's own refusal causes "
                "observed across the grid were: "
                + (",".join(qualification.checker_refusal_causes)
                   or "none")
                + ". STRONGER THAN THE LITERAL SENTENCE on two withholding "
                f"branches: '{DEFERRED_EPOCH_CLOSED_ON_INVALIDATION}' held "
                f"{invalidation_row['rows']} epoch(s) and "
                f"'{DEFERRED_POLICY_BARRIER}' held "
                f"{barrier_row['rows']}. COUNTERFACTUAL, the evidence that "
                "the strengthening is load-bearing rather than decoration: "
                "§11 E5 read literally would have published "
                f"{invalidation_row['would_publish']} of the first branch's "
                "epochs, of which "
                f"{invalidation_row['would_publish_with_disclosure_miss']} "
                "carry a disclosure the eventual timeline escapes, and "
                f"{barrier_row['would_publish']} of the second's, of which "
                f"{barrier_row['would_publish_with_disclosure_miss']} do. "
                "See manifest.publication_rule_strengthening for the §7 "
                "justification"
            ),
        ),
        FalsificationRow(
            clause=CLAUSE_STALE_PLAN_CONTINUES_ADMISSION,
            fired=bool(escaped),
            zero_tolerance=True,
            evaluated=int(published["admission_stop_probes"]),
            detail=(
                (
                    "every not-yet-started action of every invalidated "
                    "plan was ATTEMPTED against the live ledger and "
                    "refused"
                    if not escaped
                    else "OLD PLAN CONTINUED ADMISSION AFTER ITS DIGEST "
                         "WENT STALE: " + ",".join(sorted(escaped))
                )
                + f". {published['admission_stop_refusals']} refusal(s) over "
                f"{published['admission_stop_probes']} probe(s), against a "
                f"preregistered minimum of "
                f"{MINIMUM_ADMISSION_STOP_PROBES}. The directed witness "
                f"reports refused={stop_witness.refused}"
            ),
        ),
        FalsificationRow(
            clause=CLAUSE_DISCLOSURE_MISSES_EVENTUAL_LOCATION,
            fired=disclosure_established and bool(published["misses"]),
            zero_tolerance=True,
            evaluated=graded_with_reference,
            detail=(
                (
                    (
                        "every qualified publication's MayChange set "
                        "CONTAINED every location eventual full processing "
                        "still changes"
                        if not published["misses"]
                        else "DISCLOSURE MISSED AN EVENTUAL CHANGED "
                             "LOCATION: " + ",".join(published["misses"])
                    )
                    if disclosure_established
                    else "NOT EVALUATED: the zero-tolerance recall clause is "
                         "stated over publications that HAVE a reference "
                         "location, and this run produced too few of them to "
                         "distinguish a sound disclosure from an empty one"
                )
                + f". Recall {published['recall']}, precision "
                f"{published['precision']} over "
                f"{graded_with_reference} qualified publication(s) WITH a "
                f"non-empty reference set — the denominator the floor of "
                f"{MINIMUM_GRADED_PUBLICATIONS} is stated over — plus "
                f"{published['graded_without_reference']} publication(s) "
                f"whose reference set was EMPTY and which are therefore "
                f"contained vacuously and decide nothing "
                f"({published['graded']} graded in total). SECONDARY, "
                f"deciding nothing: over ALL {all_checked['graded']} checked "
                f"epoch(s) ({all_checked['graded_with_reference']} with a "
                f"reference set) recall {all_checked['recall']}, precision "
                f"{all_checked['precision']}"
            ),
        ),
        FalsificationRow(
            clause=CLAUSE_REPLANNING_COST_DOMINATES,
            fired=cost.established and cost.dominates,
            zero_tolerance=False,
            evaluated=cost.accounted,
            detail=(f"{cost.headline}. CONCLUSION: {cost.conclusion}. "
                    f"RATE SENSITIVITY: {cost.rate_sensitivity}"),
        ),
    )

    return E5Report(
        manifest_digest=manifest_digest,
        workloads=tuple(workload_rows),
        timelines=tuple(timeline_rows),
        runs=tuple(runs),
        cost=cost,
        admission_stop=stop_witness,
        qualification=qualification,
        disclosure_published=published,
        disclosure_all_checked=all_checked,
        falsifications=falsifications,
        deferred=dict(manifest.deferred),
        metric_definitions=dict(manifest.metric_definitions),
        secondary_metrics=dict(manifest.secondary_metrics),
        publication_rule_strengthening=manifest.publication_rule_strengthening,
        replan_latency_ns=tuple(latencies),
    )
