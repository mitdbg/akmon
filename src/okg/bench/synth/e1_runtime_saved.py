"""E1 completion — the trace-derived runtime-saved and checker-mutation metrics.

PACT ``concord-disclosure-budget``, task.batch.support-index-theorem
(req.batch.support-theorem). §E1 of ``designs/batch-scheduling-research.md``
names six metrics; :mod:`okg.bench.synth.e1_support_stress` delivered four and
deferred two by name to this task:

* **runtime saved** — needs per-action traces. Delivered here, from the
  measurement adapter's realized measurements
  (:func:`okg.substrate.consistency.batch_measurements.measure_replay`) over
  TWO replays of the SAME sealed history: one ``MODE_COMPLETE`` and one
  ``MODE_DIFFERENTIAL``. Runtime saved is complete-minus-differential, in the
  adapter's declared units, under the adapter's UNKNOWN discipline.
* **checker mutation detection** — delivered here, from the EXISTING parity
  safety mutations applied to the same live replay: the certificate-row
  corruption (:data:`...parity.MUTATION_CORRUPT_CERTIFICATE`) and a
  dependency-fact mutation (:data:`...parity.MUTATION_DEPENDENCY_FACT`).

Where the numbers come from, and where they do not
--------------------------------------------------
Runtime saved is a WALL-CLOCK quantity. The E1 pure half has no wall clock and
no database: its arms are exact deterministic counts over an in-memory world,
so a "runtime" there would be a number about this process's scheduler, not
about the engine. The pure arms therefore keep an honest
:data:`RUNTIME_SAVED_NOT_MEASURED_HERE` marker naming this module as the place
the measurement is taken — never a fabricated pure-world runtime, and never a
zero.

Two things are kept apart on purpose, both of which the adapter's memo
requires:

* **The cycle-0 setup interval is EXCLUDED from the compared sums and reported
  separately.** ``parity.replay_mode`` materializes the whole history's base
  corpus once, before its first ``on_update`` mark, so trace 0 declares
  :data:`...batch_measurements.MEASUREMENT_SOURCE_CYCLE_INTERVAL_WITH_SETUP` —
  a different instrument. Subtracting one arm's setup-charged interval from
  the other's would charge history materialization to the scheduler.

  **The limit of that exclusion, stated here at the definition:** it is
  LABEL-driven, not position-driven. A cycle is excluded because its realized
  measurement declares the setup-charged instrument, not because it is first.
  If a driver ever moved its one-time setup work into a LATER cycle without
  moving the label with it, that cycle would be summed into the compared total
  and charged to the scheduler, and nothing here would notice. The
  ``len(setup) > 1`` refusal below catches setup DUPLICATION (traces merged
  from two runs); it does not catch setup RELOCATION. The protection against
  relocation lives in the measurement adapter, which stamps the label from
  the interval it actually measured.
* **The two arms must be ONE experiment, and the metric says so in its
  payload.** A difference between arms that bound different corpora, code
  states, resource configurations, or granularities is two unrelated numbers
  subtracted. :func:`measure_runtime_saved` refuses unless all four agree, and
  emits all four beside the number so a stored report can be falsified after
  the fact rather than taken on trust.
* **UNKNOWN is never zero-filled.** A single unknown realized duration makes
  the difference UNKNOWN with a reason that names the offending components,
  and the whole measurement reports
  :data:`RUNTIME_SAVED_PARTIALLY_KNOWN`. The known subtotals and their
  coverage counts are disclosed beside it so a reader can see exactly how much
  of the interval the number covers.

Nothing here re-implements a mutation, a scheduler decision, or a grading
rule: the mutations are parity's own (which already refuse to apply as a
no-op), and detection is classified from what the REAL execution produced —
a named refusal raised by the certificate store, or an invalidated
dependency class plus the victim step actually re-running.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from okg.bench.synth.e1_support_stress import (
    RUNTIME_SAVED_MEASURED_BY,
    RUNTIME_SAVED_NOT_MEASURED_HERE,
    pure_arm_runtime_saved_marker,
)
from okg.bench.synth.parity import (
    MODE_COMPLETE,
    MODE_DIFFERENTIAL,
    MUTATION_CORRUPT_CERTIFICATE,
    MUTATION_DEPENDENCY_FACT,
)
from okg.substrate.consistency.batch_measurements import (
    SETUP_CHARGED_MEASUREMENT_SOURCES,
    ActionTraceSet,
)
from okg.substrate.consistency.batch_schedule import MaybeInt, Unknown
from okg.substrate.consistency.reuse_scheduler import CAUSE_CERTIFICATE_VALID
from okg.substrate.errors import SubstrateError

__all__ = [
    "DETECTION_MODES",
    "DETECTION_MODE_INVALIDATED_AND_RERAN",
    "DETECTION_MODE_NOT_CAUGHT",
    "DETECTION_MODE_REFUSED",
    "E1MetricError",
    "MutationDetection",
    "RUNTIME_SAVED_MEASURED",
    "RUNTIME_SAVED_MEASURED_BY",
    "RUNTIME_SAVED_NOT_MEASURED_HERE",
    "RUNTIME_SAVED_PARTIALLY_KNOWN",
    "RUNTIME_SAVED_STATUSES",
    "RuntimeSaved",
    "SHARED_BINDING_FIELDS",
    "classify_certificate_corruption",
    "classify_dependency_fact_mutation",
    "measure_runtime_saved",
    "pure_arm_runtime_saved_marker",
]


class E1MetricError(SubstrateError):
    """An E1 completion metric refused: a mis-built metric proves nothing."""


# --------------------------------------------------------------------------- #
# Runtime saved.                                                               #
# --------------------------------------------------------------------------- #
#: Every compared component was measured; the difference is an integer.
RUNTIME_SAVED_MEASURED = "measured"
#: At least one compared component was UNKNOWN, so the difference is UNKNOWN
#: with a reason. The known subtotals are disclosed, never summed into a
#: number that pretends to cover the unknown part.
RUNTIME_SAVED_PARTIALLY_KNOWN = "partially_known"
#: :data:`RUNTIME_SAVED_NOT_MEASURED_HERE` and the pure-arm marker live in
#: :mod:`okg.bench.synth.e1_support_stress` — the harness must be able to
#: state its own honest marker without importing the live-path machinery this
#: module needs. They are re-exported here so a reader of either module sees
#: one vocabulary.
RUNTIME_SAVED_STATUSES: tuple[str, ...] = (
    RUNTIME_SAVED_MEASURED,
    RUNTIME_SAVED_NOT_MEASURED_HERE,
    RUNTIME_SAVED_PARTIALLY_KNOWN,
)

#: The §12 binding fields both arms must agree on before a difference between
#: them is a saving rather than two unrelated numbers. Three come from every
#: trace's :class:`...batch_measurements.ReproducibilityBinding`; the fourth
#: is the trace set's declared granularity, because a publish-cycle total
#: minus an executor-step total is not a quantity at all.
SHARED_BINDING_FIELDS: tuple[str, ...] = (
    "corpus_manifest_digest",
    "granularity",
    "okg_commit",
    "resource_config_digest",
)
#: The subset of :data:`SHARED_BINDING_FIELDS` read off each trace's binding.
_PER_TRACE_BINDING_FIELDS: tuple[str, ...] = (
    "corpus_manifest_digest",
    "okg_commit",
    "resource_config_digest",
)


@dataclass(frozen=True)
class _ArmAccount:
    """One replay arm's cycle account, with setup separated out."""

    mode: str
    trace_set_id: str
    compared_cycles: int
    duration_known_subtotal: int
    duration_unknown_components: tuple[str, ...]
    cost_known_subtotal: int
    cost_unknown_components: tuple[str, ...]
    setup_duration: MaybeInt
    setup_cost: MaybeInt

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "compared_cycles": self.compared_cycles,
            "cost_known_subtotal": self.cost_known_subtotal,
            "cost_unknown_components": list(self.cost_unknown_components),
            "duration_known_subtotal": self.duration_known_subtotal,
            "duration_unknown_components": list(
                self.duration_unknown_components
            ),
            "mode": self.mode,
            "setup_cost": _quantity(self.setup_cost),
            "setup_duration": _quantity(self.setup_duration),
            "trace_set_id": self.trace_set_id,
        }


@dataclass(frozen=True)
class RuntimeSaved:
    """The §E1 runtime-saved metric, derived from per-action traces.

    ``duration_saved`` and ``cost_saved`` are complete-minus-differential over
    the SAME sealed history, excluding the setup-charged cycle-0 interval,
    which each arm reports separately. Either may be a
    :class:`...batch_schedule.Unknown` — an unmeasured component is never
    zero-filled — and either may legitimately be NEGATIVE, which is reported
    as measured rather than clamped: differential execution being slower on a
    short history is a result, not an error."""

    history_binding: str
    duration_unit: str
    cost_unit: str
    #: The §12 bindings BOTH arms agreed on — emitted so a stored number can
    #: be checked against the experiment it claims to describe.
    shared_bindings: Mapping[str, str]
    complete: _ArmAccount
    differential: _ArmAccount
    duration_saved: MaybeInt
    cost_saved: MaybeInt
    status: str

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "complete": self.complete.to_canonical_dict(),
            "cost_saved": _quantity(self.cost_saved),
            "cost_unit": self.cost_unit,
            "differential": self.differential.to_canonical_dict(),
            "duration_saved": _quantity(self.duration_saved),
            "duration_unit": self.duration_unit,
            "history_binding": self.history_binding,
            "schema": "okg.synth.e1-runtime-saved.v1",
            "setup_disclosure": (
                "cycle 0 declares the setup-charged instrument "
                "(one-time whole-history corpus materialization) and is "
                "EXCLUDED from both compared sums; each arm reports its own "
                "setup interval separately. The exclusion is LABEL-driven, "
                "not position-driven: a driver that relocated its one-time "
                "setup into a later cycle WITHOUT moving the "
                "setup-charged measurement source with it would have that "
                "cycle summed into the compared total and charged to the "
                "scheduler; the duplicate-setup refusal does not catch "
                "relocation"
            ),
            "shared_bindings": dict(sorted(self.shared_bindings.items())),
            "status": self.status,
        }

    def canonical_json(self) -> str:
        return json.dumps(
            self.to_canonical_dict(), sort_keys=True, separators=(",", ":")
        )


def _quantity(value: MaybeInt) -> Any:
    if isinstance(value, Unknown):
        return value.to_canonical_dict()
    return value


def _require_traces(trace_set: ActionTraceSet, label: str) -> tuple[Any, ...]:
    traces = tuple(trace_set.traces)
    if not traces:
        raise E1MetricError(
            "an empty trace set measures nothing — a runtime-saved metric "
            "over no cycles would be an unearned zero",
            data={"arm": label, "trace_set_id": trace_set.trace_set_id},
        )
    return traces


def _arm_bindings(trace_set: ActionTraceSet, *, arm: str) -> dict[str, str]:
    """The one §12 binding this arm carries, or a refusal.

    An arm whose traces disagree with each other is not one replay of one
    history, so it has no binding to compare with the other arm's. This is
    the per-arm half; the cross-arm half is in
    :func:`measure_runtime_saved`, and neither implies the other."""
    traces = _require_traces(trace_set, arm)
    bindings: dict[str, str] = {"granularity": trace_set.granularity}
    for field_name in _PER_TRACE_BINDING_FIELDS:
        seen = {str(getattr(t.binding, field_name)) for t in traces}
        if len(seen) > 1:
            raise E1MetricError(
                f"an arm's traces bind more than one {field_name} — the "
                "arm is not one replay of one history",
                data={"arm": arm, "field": field_name,
                      "values": sorted(seen)},
            )
        bindings[field_name] = seen.pop()
    return bindings


def _account(
    trace_set: ActionTraceSet, *, expected_mode: str, label: str
) -> _ArmAccount:
    traces = _require_traces(trace_set, label)
    modes = {t.provenance.get("mode", "") for t in traces}
    if modes != {expected_mode}:
        raise E1MetricError(
            "the arm's traces are not all from the expected execution mode; "
            "runtime saved compares ONE mode against ONE other mode",
            data={"arm": label, "expected": expected_mode,
                  "found": sorted(modes)},
        )

    setup: list[Any] = []
    compared: list[Any] = []
    for trace in traces:
        if trace.realized.measurement_source in SETUP_CHARGED_MEASUREMENT_SOURCES:
            setup.append(trace)
        else:
            compared.append(trace)
    if len(setup) > 1:
        raise E1MetricError(
            "more than one setup-charged interval in one replay — the "
            "setup instrument is the FIRST interval only, so a second one "
            "means the traces were merged from two runs",
            data={"arm": label,
                  "setup_action_ids": [t.action_id for t in setup]},
        )
    if not compared:
        raise E1MetricError(
            "every cycle in this arm carried the setup-charged instrument, "
            "so nothing comparable remains; lengthen the history",
            data={"arm": label},
        )

    duration_total = 0
    duration_unknown: list[str] = []
    cost_total = 0
    cost_unknown: list[str] = []
    for trace in compared:
        duration = trace.realized.duration
        if isinstance(duration, Unknown):
            duration_unknown.append(f"{trace.action_id}: {duration.reason}")
        else:
            duration_total += duration
        cost = trace.realized.cost
        if isinstance(cost, Unknown):
            cost_unknown.append(f"{trace.action_id}: {cost.reason}")
        else:
            cost_total += cost

    return _ArmAccount(
        mode=expected_mode,
        trace_set_id=trace_set.trace_set_id,
        compared_cycles=len(compared),
        duration_known_subtotal=duration_total,
        duration_unknown_components=tuple(duration_unknown),
        cost_known_subtotal=cost_total,
        cost_unknown_components=tuple(cost_unknown),
        setup_duration=(
            setup[0].realized.duration if setup
            else Unknown("this arm produced no setup-charged interval")
        ),
        setup_cost=(
            setup[0].realized.cost if setup
            else Unknown("this arm produced no setup-charged interval")
        ),
    )


def _difference(
    complete_known: int,
    complete_unknown: Sequence[str],
    differential_known: int,
    differential_unknown: Sequence[str],
    *,
    quantity: str,
) -> MaybeInt:
    if complete_unknown or differential_unknown:
        return Unknown(
            f"{quantity} saved is not established: "
            f"{len(complete_unknown)} complete-mode and "
            f"{len(differential_unknown)} differential-mode component(s) "
            f"were UNKNOWN "
            f"({'; '.join([*complete_unknown, *differential_unknown])[:400]})"
        )
    return complete_known - differential_known


def measure_runtime_saved(
    complete: Any,
    differential: Any,
    *,
    history_binding: str,
) -> RuntimeSaved:
    """Runtime saved = complete realized minus differential realized.

    ``complete`` and ``differential`` are the
    :class:`...batch_measurements.MeasuredReplay` results of replaying the
    SAME sealed history in ``MODE_COMPLETE`` and ``MODE_DIFFERENTIAL``. Both
    arms must declare the same units and cover the same number of update
    cycles; anything else is not a difference, it is two unrelated numbers
    subtracted, and this function refuses it.

    Both arms must further be ONE experiment. That is not implied by matching
    units or matching cycle counts: two replays of DIFFERENT corpora, at
    different commits, under different resource configurations, or at
    different granularities all pass those checks and produce a difference
    that means nothing. So all four of :data:`SHARED_BINDING_FIELDS` must
    agree across the arms, and all four are emitted in the payload — a stored
    saving that names no experiment cannot be falsified later.

    ``history_binding`` identifies the sealed history both arms replayed. It
    must BE the corpus manifest digest both arms bound, not free text beside
    it: a label the traces do not corroborate binds nothing."""
    if not history_binding:
        raise E1MetricError(
            "runtime saved must bind the sealed history both arms replayed; "
            "an unbound saving cannot be reproduced or compared"
        )
    complete_set = complete.cycle_traces
    differential_set = differential.cycle_traces
    if complete_set.duration_unit != differential_set.duration_unit:
        raise E1MetricError(
            "unlike duration units may not be subtracted",
            data={"complete": complete_set.duration_unit,
                  "differential": differential_set.duration_unit},
        )
    if complete_set.cost_unit != differential_set.cost_unit:
        raise E1MetricError(
            "unlike cost units may not be subtracted",
            data={"complete": complete_set.cost_unit,
                  "differential": differential_set.cost_unit},
        )
    complete_bindings = _arm_bindings(complete_set, arm="complete")
    differential_bindings = _arm_bindings(differential_set, arm="differential")
    disagreeing = sorted(
        field_name for field_name in SHARED_BINDING_FIELDS
        if complete_bindings[field_name] != differential_bindings[field_name]
    )
    if disagreeing:
        raise E1MetricError(
            "the two arms are not the same experiment: they disagree on "
            f"{disagreeing}, so their difference is not a saving but two "
            "unrelated numbers subtracted",
            data={
                "disagreeing": {
                    field_name: {
                        "complete": complete_bindings[field_name],
                        "differential": differential_bindings[field_name],
                    }
                    for field_name in disagreeing
                },
            },
        )
    shared_bindings = dict(complete_bindings)
    if history_binding != shared_bindings["corpus_manifest_digest"]:
        raise E1MetricError(
            "history_binding must BE the corpus manifest digest both arms "
            "bound, not free text beside it; an uncorroborated label lets a "
            "saving be re-read against a history it never replayed",
            data={
                "history_binding": history_binding,
                "corpus_manifest_digest": (
                    shared_bindings["corpus_manifest_digest"]
                ),
            },
        )

    complete_account = _account(
        complete_set, expected_mode=MODE_COMPLETE, label="complete",
    )
    differential_account = _account(
        differential_set, expected_mode=MODE_DIFFERENTIAL,
        label="differential",
    )
    if complete_account.compared_cycles != differential_account.compared_cycles:
        raise E1MetricError(
            "the two arms compared a different number of update cycles, so "
            "their difference is not a saving on one history",
            data={"complete": complete_account.compared_cycles,
                  "differential": differential_account.compared_cycles},
        )

    duration_saved = _difference(
        complete_account.duration_known_subtotal,
        complete_account.duration_unknown_components,
        differential_account.duration_known_subtotal,
        differential_account.duration_unknown_components,
        quantity="duration",
    )
    cost_saved = _difference(
        complete_account.cost_known_subtotal,
        complete_account.cost_unknown_components,
        differential_account.cost_known_subtotal,
        differential_account.cost_unknown_components,
        quantity="cost",
    )
    status = (
        RUNTIME_SAVED_PARTIALLY_KNOWN
        if isinstance(duration_saved, Unknown) or isinstance(cost_saved, Unknown)
        else RUNTIME_SAVED_MEASURED
    )
    return RuntimeSaved(
        history_binding=history_binding,
        duration_unit=complete_set.duration_unit,
        cost_unit=complete_set.cost_unit,
        shared_bindings=shared_bindings,
        complete=complete_account,
        differential=differential_account,
        duration_saved=duration_saved,
        cost_saved=cost_saved,
        status=status,
    )


# --------------------------------------------------------------------------- #
# Checker mutation detection.                                                  #
# --------------------------------------------------------------------------- #
#: The mutation was caught because a named check REFUSED and raised.
DETECTION_MODE_REFUSED = "refused_by_a_named_check"
#: The mutation was caught because the certificate invalidated on the moved
#: dependency class AND the victim step actually re-ran.
DETECTION_MODE_INVALIDATED_AND_RERAN = "certificate_invalidated_and_step_reran"
#: The mutation ran and nothing caught it. Reported as a finding, never
#: silently dropped.
DETECTION_MODE_NOT_CAUGHT = "not_caught"

DETECTION_MODES: tuple[str, ...] = (
    DETECTION_MODE_INVALIDATED_AND_RERAN,
    DETECTION_MODE_NOT_CAUGHT,
    DETECTION_MODE_REFUSED,
)

#: The certificate store's own refusal markers (``reuse_certificates``): a
#: stored row whose content moved after write fails the recomputed-digest
#: check. All three must appear, so an unrelated failure cannot be counted as
#: a detection.
CORRUPTION_REFUSAL_MARKERS: tuple[str, ...] = (
    "stored reuse certificate digest",
    "recomputed digest",
    "corrupted after write",
)


@dataclass(frozen=True)
class MutationDetection:
    """One checker mutation's graded detection outcome.

    ``mutation_evidence`` records what proves the mutation ACTUALLY mutated
    something — a no-op mutation proves nothing, and both classifiers below
    refuse to grade one."""

    mutation_kind: str
    update_index: int
    target_step: str
    detected: bool
    detection_mode: str
    refusal: str
    mutation_evidence: Mapping[str, str]

    def __post_init__(self) -> None:
        if self.detection_mode not in DETECTION_MODES:
            raise E1MetricError(
                "unknown detection mode — the taxonomy is closed",
                data={"got": self.detection_mode,
                      "known": list(DETECTION_MODES)},
            )
        if self.detected == (self.detection_mode == DETECTION_MODE_NOT_CAUGHT):
            raise E1MetricError(
                "detected and detection_mode disagree; a detection must name "
                "HOW it was detected",
                data={"detected": self.detected,
                      "detection_mode": self.detection_mode},
            )
        if self.detected and not self.refusal:
            raise E1MetricError(
                "a detection must name the refusal or invalidation that "
                "caught it; an unnamed catch is not evidence",
                data={"mutation_kind": self.mutation_kind},
            )

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "detected": self.detected,
            "detection_mode": self.detection_mode,
            "mutation_evidence": dict(sorted(self.mutation_evidence.items())),
            "mutation_kind": self.mutation_kind,
            "refusal": self.refusal,
            "target_step": self.target_step,
            "update_index": self.update_index,
        }


def _require_applied(mutation: Any, required_detail: Sequence[str]) -> None:
    """A mutation that did not apply grades nothing (parity's own rule)."""
    if not getattr(mutation, "target_step", ""):
        raise E1MetricError(
            "the mutation never named a target step — it did not apply, so "
            "neither a detection nor a miss can be attributed to it",
            data={"kind": getattr(mutation, "kind", "")},
        )
    detail = dict(getattr(mutation, "detail", {}) or {})
    missing = [key for key in required_detail if key not in detail]
    if missing:
        raise E1MetricError(
            "the mutation recorded no evidence that it changed anything; a "
            "mutation that mutates nothing proves nothing",
            data={"kind": getattr(mutation, "kind", ""), "missing": missing},
        )


def classify_certificate_corruption(
    *, mutation: Any, update_index: int, raised: BaseException | None,
) -> MutationDetection:
    """Grade the certificate-row corruption mutation.

    The store recomputes a stored certificate's digest on read and refuses a
    row whose content moved after write. Detection is therefore that named
    refusal reaching the caller — ALL of
    :data:`CORRUPTION_REFUSAL_MARKERS` must appear in the raised message, so
    an unrelated failure (a missing table, a connection drop) can never be
    banked as a detection."""
    _require_applied(mutation, ("corrupted_step",))
    corrupted = str(mutation.detail["corrupted_step"])
    if corrupted != mutation.target_step:
        raise E1MetricError(
            "the corruption mutation's target and corrupted step disagree",
            data={"target": mutation.target_step, "corrupted": corrupted},
        )
    evidence = {
        "corrupted_step": corrupted,
        "row_update_matched_exactly_one_row": (
            "parity._corrupt_certificate_row refuses any rowcount != 1"
        ),
    }
    if raised is None:
        return MutationDetection(
            mutation_kind=MUTATION_CORRUPT_CERTIFICATE,
            update_index=int(update_index), target_step=corrupted,
            detected=False, detection_mode=DETECTION_MODE_NOT_CAUGHT,
            refusal="", mutation_evidence=evidence,
        )
    message = str(raised)
    missing = [m for m in CORRUPTION_REFUSAL_MARKERS if m not in message]
    if missing:
        return MutationDetection(
            mutation_kind=MUTATION_CORRUPT_CERTIFICATE,
            update_index=int(update_index), target_step=corrupted,
            detected=False, detection_mode=DETECTION_MODE_NOT_CAUGHT,
            refusal="",
            mutation_evidence={
                **evidence,
                "raised_but_unattributable": (
                    f"{type(raised).__name__}: missing markers "
                    f"{missing}"
                ),
            },
        )
    return MutationDetection(
        mutation_kind=MUTATION_CORRUPT_CERTIFICATE,
        update_index=int(update_index), target_step=corrupted,
        detected=True, detection_mode=DETECTION_MODE_REFUSED,
        refusal=(
            f"{type(raised).__name__}: stored certificate digest does not "
            "match the recomputed digest (corrupted after write)"
        ),
        mutation_evidence=evidence,
    )


def classify_dependency_fact_mutation(
    *, mutation: Any, update_index: int, control: Any, mutated: Any,
) -> MutationDetection:
    """Grade a dependency-fact mutation on the same live replay.

    ``control`` and ``mutated`` are the ``UpdateExecution`` for the same
    update without and with the mutation. The mutation re-authors the
    deployment's constraint set UNDER a valid certificate, so detection is:
    the certificate invalidated on ``constraint_set``, and the victim step
    actually RE-RAN. A control that did not skip behind a valid certificate
    is refused — mutating a step that was going to run anyway proves
    nothing."""
    _require_applied(mutation, ("mutated_class",))
    mutated_class = str(mutation.detail["mutated_class"])
    if CAUSE_CERTIFICATE_VALID not in control.ledger.skips_by_cause:
        raise E1MetricError(
            "the control update did not skip behind a valid certificate, so "
            "moving a dependency class under it proves nothing; choose an "
            "update the ladder actually reuses",
            data={"update": int(update_index),
                  "control_skip_causes": dict(control.ledger.skips_by_cause)},
        )
    evidence = {
        "mutated_class": mutated_class,
        "control_skipped_behind_a_valid_certificate": str(
            control.ledger.skips_by_cause[CAUSE_CERTIFICATE_VALID]
        ),
        "control_steps_run": str(control.ledger.steps_run),
        "mutated_steps_run": str(mutated.ledger.steps_run),
    }
    invalidated = mutated_class in mutated.ledger.invalidated_classes
    reran = mutation.target_step in mutated.run_steps
    if invalidated and reran and mutated.ledger.certificates_invalidated > 0:
        return MutationDetection(
            mutation_kind=MUTATION_DEPENDENCY_FACT,
            update_index=int(update_index),
            target_step=mutation.target_step,
            detected=True,
            detection_mode=DETECTION_MODE_INVALIDATED_AND_RERAN,
            refusal=(
                f"certificate invalidated on dependency class "
                f"{mutated_class!r}; {mutation.target_step} re-ran"
            ),
            mutation_evidence=evidence,
        )
    return MutationDetection(
        mutation_kind=MUTATION_DEPENDENCY_FACT,
        update_index=int(update_index), target_step=mutation.target_step,
        detected=False, detection_mode=DETECTION_MODE_NOT_CAUGHT, refusal="",
        mutation_evidence={
            **evidence,
            "invalidated_on_the_mutated_class": str(invalidated),
            "victim_reran": str(reran),
            "certificates_invalidated": str(
                mutated.ledger.certificates_invalidated
            ),
        },
    )
