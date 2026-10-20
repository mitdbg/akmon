"""PACT ``concord-disclosure-budget``, task.batch.measurement-adapter —
the per-action measurement witnesses.

Covers the task's verification clause on both halves.

Pure half: the trace schema round-trips through its canonical form with a
stable digest; the UNKNOWN discipline holds (an absent estimate is UNKNOWN
and never zero, a realized zero-length interval refuses, unlike units
refuse, every declared resource is answered for); the estimate /
reservation / realized separation is STRUCTURAL rather than
conventional; the §12 bindings are present and covered by the digest; the
conversion produces action fields the FROZEN contract's own loader
accepts; and the clock is injectable, so a measurement is deterministic.

DB half (``@pytest.mark.postgres``): a short live parity replay through
``parity.replay_mode``, wrapped by the adapter, yields one realized trace
per executed action with a positive realized duration, no realized trace
for a skipped step, §12 bindings that match the live repository — checked
against ``git`` read INDEPENDENTLY of the binding function, since
comparing that function with itself proves only determinism — a first
cycle whose instrument label names the one-time driver setup its interval
carries, and parity verdicts EQUAL to an unwrapped run's: the
unchanged-semantics claim asserted, not assumed.

Design references: ``designs/batch-scheduling-research.md`` §7 (+§7.1,
§7.2), §12, §13 step 3, and §4.3's named gap.
"""
from __future__ import annotations

import ast
import copy
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable, Sequence

import pytest

from okg.substrate.consistency import batch_measurements, reuse_scheduler
from okg.substrate.consistency.batch_measurements import (
    COST_UNIT_EXECUTOR_STEPS,
    DURATION_UNIT_NANOSECONDS,
    ESTIMATOR_UNESTIMATED,
    FIELD_SOURCE_ESTIMATE,
    FIELD_SOURCE_REALIZED_RECALIBRATION,
    GRANULARITY_EXECUTOR_STEP,
    GRANULARITY_PUBLISH_CYCLE,
    MEASUREMENT_SCHEMA_VERSION,
    MEASUREMENT_SOURCE_CYCLE_INTERVAL,
    MEASUREMENT_SOURCE_CYCLE_INTERVAL_WITH_SETUP,
    PARITY_RESOURCES,
    PARITY_RESOURCE_CERTIFICATE_WRITES,
    PARITY_RESOURCE_EXECUTOR_STEPS,
    PARITY_RESOURCE_PUBLISH_COMMITS,
    REFUSAL_ADAPTER_SURFACE,
    REFUSAL_DUPLICATE_IDENTITY,
    REFUSAL_ESTIMATOR_DISAGREEMENT,
    REFUSAL_MISSING_RESOURCE_DEMAND,
    REFUSAL_MIXED_COST_UNITS,
    REFUSAL_MIXED_DURATION_UNITS,
    REFUSAL_MISSING_FIELD,
    REFUSAL_NON_CANONICAL_VALUE,
    REFUSAL_REALIZED_AS_ESTIMATE,
    REFUSAL_SETUP_CHARGED_REALIZED,
    REFUSAL_UNDECLARED_RESOURCE,
    REFUSAL_UNKNOWN_FIELD,
    REFUSAL_UNKNOWN_FIELD_SOURCE,
    REFUSAL_UNKNOWN_GRANULARITY,
    REFUSAL_UNMEASURED_ZERO,
    TREE_CLEAN,
    TREE_DIRTY,
    ActionTrace,
    ActionTraceSet,
    ContractActionFields,
    MeasurementRefused,
    PlanEstimate,
    RealizedMeasurement,
    ReproducibilityBinding,
    Reservation,
    contract_action_fields,
    cycle_trace_from_execution,
    load_trace_set,
    measure_replay,
    okg_repo_binding,
    resource_declaration_digest,
    skipped_step_ids,
    step_traces_from_execution,
)
from okg.substrate.consistency.batch_schedule import (
    CONTRACT_SCHEMA_VERSION,
    OUTCOME_SUCCESS,
    UNKNOWN,
    EstimatorRef,
    Unknown,
    load_batch_instance,
)

# --------------------------------------------------------------------------- #
# Fixtures: a hand-built trace, and a fake execution shaped like the real one. #
# --------------------------------------------------------------------------- #
_RECALIBRATED = EstimatorRef(id="concord.batch.replay_recalibration", version=1)


def _binding(**overrides: Any) -> ReproducibilityBinding:
    base = dict(
        okg_commit="0" * 40,
        okg_tree_status=TREE_CLEAN,
        corpus_manifest_digest="a" * 64,
        resource_config_version=3,
        resource_config_digest="b" * 64,
        machine_runtime_label="darwin-arm64-cpython3.12",
    )
    base.update(overrides)
    return ReproducibilityBinding(**base)  # type: ignore[arg-type]


def _trace(action_id: str = "cycle-1", **overrides: Any) -> ActionTrace:
    base: dict[str, Any] = dict(
        action_id=action_id,
        action_class=GRANULARITY_PUBLISH_CYCLE,
        cost_unit=COST_UNIT_EXECUTOR_STEPS,
        duration_unit=DURATION_UNIT_NANOSECONDS,
        estimate=PlanEstimate.unestimated("no estimator exists yet"),
        reservation=Unknown("the replay path enforces no cap"),
        realized=RealizedMeasurement(
            duration=1_250_000, cost=7, measurement_source="test-clock",
        ),
        demand={
            PARITY_RESOURCE_CERTIFICATE_WRITES: 2,
            PARITY_RESOURCE_EXECUTOR_STEPS: 7,
            PARITY_RESOURCE_PUBLISH_COMMITS: 1,
        },
        outcome=OUTCOME_SUCCESS,
        binding=_binding(),
        provenance={"mode": "differential", "update": "3"},
    )
    base.update(overrides)
    return ActionTrace(**base)


def _set(*traces: ActionTrace, **overrides: Any) -> ActionTraceSet:
    base: dict[str, Any] = dict(
        schema_version=MEASUREMENT_SCHEMA_VERSION,
        trace_set_id="witness/publish_cycle",
        epoch=0,
        granularity=GRANULARITY_PUBLISH_CYCLE,
        cost_unit=COST_UNIT_EXECUTOR_STEPS,
        duration_unit=DURATION_UNIT_NANOSECONDS,
        declared_resources=PARITY_RESOURCES,
        traces=traces or (_trace(),),
    )
    base.update(overrides)
    return ActionTraceSet(**base)  # type: ignore[arg-type]


class _FakeLedger:
    """The subset of ``parity.LedgerRow`` the adapter reads.

    The default terminal is the SCHEDULER's own constant, not a literal.
    A fixture that restated ``"fixed_point"`` would check the adapter's
    copy against the fixture's copy — a mirror against a mirror — and
    would stay green through exactly the drift that matters."""

    def __init__(self, *, steps_run: int, steps_reached: int,
                 certificates_written: int = 0,
                 terminal: str = reuse_scheduler.TERMINAL_FIXED_POINT,
                 fallback_cause: str | None = None, published: bool = True,
                 generation_id: str = "gen-1") -> None:
        self.steps_run = steps_run
        self.steps_reached = steps_reached
        self.certificates_written = certificates_written
        self.terminal = terminal
        self.fallback_cause = fallback_cause
        self.published = published
        self.generation_id = generation_id


class _FakeExecution:
    """The subset of ``parity.UpdateExecution`` the adapter reads."""

    def __init__(self, *, mode: str = "differential", index: int = 2,
                 run_steps: Sequence[str] = ("s:a", "s:b"),
                 skip_outcomes: Sequence[tuple[str, str]] = (("s:c", "cert"),),
                 ledger: _FakeLedger | None = None) -> None:
        self.mode = mode
        self.index = index
        self.run_steps = tuple(run_steps)
        self.skip_outcomes = tuple(skip_outcomes)
        self.ledger = ledger or _FakeLedger(
            steps_run=len(run_steps),
            steps_reached=len(run_steps) + len(skip_outcomes),
            certificates_written=1,
        )


class _StepClock:
    """A deterministic clock: each read advances by a fixed step."""

    def __init__(self, start: int = 1_000, step: int = 500) -> None:
        self.now = start
        self.step = step

    def __call__(self) -> int:
        value = self.now
        self.now += self.step
        return value


# --------------------------------------------------------------------------- #
# Schema round-trip and digest stability.                                      #
# --------------------------------------------------------------------------- #
def test_a_trace_set_round_trips_through_its_canonical_form() -> None:
    original = _set(_trace("cycle-1"), _trace("cycle-2"))
    reloaded = load_trace_set(original.to_canonical_dict())
    assert reloaded.digest == original.digest
    assert reloaded.to_canonical_dict() == original.to_canonical_dict()


def test_the_digest_is_stable_under_declaration_order() -> None:
    forward = _set(_trace("cycle-1"), _trace("cycle-2"))
    backward = _set(_trace("cycle-2"), _trace("cycle-1"))
    assert forward.digest == backward.digest, (
        "digest identity must not depend on the order traces were declared"
    )


def test_the_digest_moves_when_any_measured_quantity_moves() -> None:
    base = _set()
    moved = _set(_trace(realized=RealizedMeasurement(
        duration=1_250_001, cost=7, measurement_source="test-clock")))
    assert moved.digest != base.digest


@pytest.mark.parametrize("field_name,value", [
    ("okg_commit", "1" * 40),
    ("okg_tree_status", TREE_DIRTY),
    ("corpus_manifest_digest", "c" * 64),
    ("resource_config_version", 4),
    ("resource_config_digest", "d" * 64),
    ("machine_runtime_label", "linux-x86_64-cpython3.12"),
    ("contract_schema_version", "batch-schedule/99"),
])
def test_every_section_12_binding_is_covered_by_the_digest(
    field_name: str, value: Any
) -> None:
    """A binding outside the digest could be swapped after measurement, so a
    dirty-tree run could be quoted as a clean-tree result."""
    base = _set()
    mutated = _set(_trace(binding=_binding(**{field_name: value})))
    assert mutated.digest != base.digest, (
        f"binding field {field_name!r} is not covered by the trace digest"
    )


def test_the_binding_carries_the_frozen_contract_version_by_default() -> None:
    assert _binding().contract_schema_version == CONTRACT_SCHEMA_VERSION


# --------------------------------------------------------------------------- #
# The UNKNOWN discipline.                                                      #
# --------------------------------------------------------------------------- #
def test_an_absent_estimate_is_unknown_and_never_zero() -> None:
    estimate = PlanEstimate.unestimated("no estimator exists yet")
    assert isinstance(estimate.duration, Unknown)
    assert isinstance(estimate.cost, Unknown)
    assert estimate.duration != 0 and estimate.cost != 0
    assert estimate.estimator == ESTIMATOR_UNESTIMATED


def test_a_stated_number_under_the_no_estimator_identity_refuses() -> None:
    with pytest.raises(MeasurementRefused) as excinfo:
        PlanEstimate(duration=UNKNOWN, cost=12,
                     estimator=ESTIMATOR_UNESTIMATED)
    assert excinfo.value.data["code"] == REFUSAL_ESTIMATOR_DISAGREEMENT


def test_a_realized_duration_of_zero_refuses_as_unresolved() -> None:
    with pytest.raises(MeasurementRefused) as excinfo:
        RealizedMeasurement(duration=0, cost=1, measurement_source="clock")
    assert excinfo.value.data["code"] == REFUSAL_UNMEASURED_ZERO


def test_an_unknown_realized_duration_states_its_reason() -> None:
    measured = RealizedMeasurement(
        duration=Unknown("the clock could not resolve this interval"),
        cost=1, measurement_source="clock",
    )
    assert measured.to_canonical_dict()["duration"] == {
        "unknown": "the clock could not resolve this interval"
    }


def test_an_omitted_declared_resource_refuses_rather_than_reading_zero() -> None:
    partial = _trace(demand={PARITY_RESOURCE_EXECUTOR_STEPS: 7})
    with pytest.raises(MeasurementRefused) as excinfo:
        _set(partial)
    assert excinfo.value.data["code"] == REFUSAL_MISSING_RESOURCE_DEMAND


def test_a_zero_demand_is_written_not_implied() -> None:
    trace = _trace(demand={
        PARITY_RESOURCE_CERTIFICATE_WRITES: 0,
        PARITY_RESOURCE_EXECUTOR_STEPS: 7,
        PARITY_RESOURCE_PUBLISH_COMMITS: 0,
    })
    canonical = _set(trace).to_canonical_dict()["traces"][0]["demand"]
    assert canonical[PARITY_RESOURCE_CERTIFICATE_WRITES] == 0
    assert set(canonical) == set(PARITY_RESOURCES)


def test_an_undeclared_resource_refuses() -> None:
    trace = _trace(demand={
        PARITY_RESOURCE_CERTIFICATE_WRITES: 0,
        PARITY_RESOURCE_EXECUTOR_STEPS: 7,
        PARITY_RESOURCE_PUBLISH_COMMITS: 1,
        "model_concurrency": 4,
    })
    with pytest.raises(MeasurementRefused) as excinfo:
        _set(trace)
    assert excinfo.value.data["code"] == REFUSAL_UNDECLARED_RESOURCE


def test_a_reservation_cap_on_an_undeclared_resource_refuses() -> None:
    trace = _trace(reservation=Reservation(
        duration_cap=UNKNOWN, cost_cap=100,
        demand_caps={"model_concurrency": 4}, enforced_by="admission-gate",
    ))
    with pytest.raises(MeasurementRefused) as excinfo:
        _set(trace)
    assert excinfo.value.data["code"] == REFUSAL_UNDECLARED_RESOURCE


# --------------------------------------------------------------------------- #
# Unit discipline: unlike units are never summed.                              #
# --------------------------------------------------------------------------- #
def test_a_trace_with_a_foreign_cost_unit_refuses() -> None:
    with pytest.raises(MeasurementRefused) as excinfo:
        _set(_trace(cost_unit="usd_cents"))
    assert excinfo.value.data["code"] == REFUSAL_MIXED_COST_UNITS


def test_a_trace_with_a_foreign_duration_unit_refuses() -> None:
    with pytest.raises(MeasurementRefused) as excinfo:
        _set(_trace(duration_unit="milliseconds"))
    assert excinfo.value.data["code"] == REFUSAL_MIXED_DURATION_UNITS


def test_converting_sets_with_mixed_cost_units_refuses() -> None:
    tokens = _set(_trace("cycle-1", cost_unit="model_tokens"),
                  cost_unit="model_tokens", trace_set_id="tokens")
    steps = _set(_trace("cycle-2"), trace_set_id="steps")
    with pytest.raises(MeasurementRefused) as excinfo:
        contract_action_fields([steps, tokens])
    assert excinfo.value.data["code"] == REFUSAL_MIXED_COST_UNITS


def test_converting_sets_with_mixed_duration_units_refuses() -> None:
    millis = _set(_trace("cycle-1", duration_unit="milliseconds"),
                  duration_unit="milliseconds", trace_set_id="ms")
    nanos = _set(_trace("cycle-2"), trace_set_id="ns")
    with pytest.raises(MeasurementRefused) as excinfo:
        contract_action_fields([nanos, millis])
    assert excinfo.value.data["code"] == REFUSAL_MIXED_DURATION_UNITS


def test_the_same_action_traced_in_two_sets_refuses() -> None:
    with pytest.raises(MeasurementRefused) as excinfo:
        contract_action_fields([_set(_trace("cycle-1"), trace_set_id="a"),
                                _set(_trace("cycle-1"), trace_set_id="b")])
    assert excinfo.value.data["code"] == REFUSAL_DUPLICATE_IDENTITY


def test_a_mixed_granularity_set_refuses() -> None:
    step_like = _trace("step-1", action_class=GRANULARITY_EXECUTOR_STEP)
    with pytest.raises(MeasurementRefused) as excinfo:
        _set(_trace("cycle-1"), step_like)
    assert excinfo.value.data["code"] == REFUSAL_UNKNOWN_GRANULARITY


# --------------------------------------------------------------------------- #
# Estimate / reservation / realized separation is STRUCTURAL.                  #
# --------------------------------------------------------------------------- #
def test_the_three_quantities_are_three_distinct_wire_objects() -> None:
    canonical = _trace().to_canonical_dict()
    assert set(canonical["estimate"]) == {"cost", "duration", "estimator"}
    assert set(canonical["realized"]) == {
        "cost", "duration", "measurement_source"}
    assert "measurement_source" not in canonical["estimate"]
    assert "estimator" not in canonical["realized"]
    assert "unknown" in canonical["reservation"]


def test_an_estimate_carrying_a_measurement_source_refuses_by_name() -> None:
    raw = _set().to_canonical_dict()
    raw["traces"][0]["estimate"]["measurement_source"] = "test-clock"
    with pytest.raises(MeasurementRefused) as excinfo:
        load_trace_set(raw)
    assert excinfo.value.data["code"] == REFUSAL_REALIZED_AS_ESTIMATE, (
        "presenting an observation as a plan input must be refused by NAME, "
        "not as a generic unknown field"
    )


def test_a_measurement_carrying_an_estimator_refuses_by_name() -> None:
    raw = _set().to_canonical_dict()
    raw["traces"][0]["realized"]["estimator"] = {"id": "guess", "version": 1}
    with pytest.raises(MeasurementRefused) as excinfo:
        load_trace_set(raw)
    assert excinfo.value.data["code"] == REFUSAL_REALIZED_AS_ESTIMATE


def test_an_unknown_trace_field_refuses() -> None:
    raw = _set().to_canonical_dict()
    raw["traces"][0]["realized_duration_ms"] = 12
    with pytest.raises(MeasurementRefused) as excinfo:
        load_trace_set(raw)
    assert excinfo.value.data["code"] == REFUSAL_UNKNOWN_FIELD


def test_realized_values_never_leak_into_the_contracts_estimate_fields() -> None:
    """The default conversion reads the ESTIMATE. On a path with no
    estimator that is UNKNOWN, even though a realized value exists."""
    converted = contract_action_fields(_set())
    fragment = converted.fields["cycle-1"]
    assert fragment["duration"]["unknown"]
    assert fragment["execution_cost"]["unknown"]
    assert fragment["estimator"] == ESTIMATOR_UNESTIMATED.to_canonical_dict()
    assert converted.field_source == FIELD_SOURCE_ESTIMATE


def test_recalibrating_from_measurements_must_name_its_estimator() -> None:
    with pytest.raises(MeasurementRefused) as excinfo:
        contract_action_fields(
            _set(), field_source=FIELD_SOURCE_REALIZED_RECALIBRATION)
    assert excinfo.value.data["code"] == REFUSAL_REALIZED_AS_ESTIMATE


def test_recalibration_cannot_borrow_the_no_estimator_identity() -> None:
    with pytest.raises(MeasurementRefused) as excinfo:
        contract_action_fields(
            _set(), field_source=FIELD_SOURCE_REALIZED_RECALIBRATION,
            recalibration_estimator=ESTIMATOR_UNESTIMATED)
    assert excinfo.value.data["code"] == REFUSAL_ESTIMATOR_DISAGREEMENT


def test_a_named_recalibration_is_visible_in_the_converted_fields() -> None:
    converted = contract_action_fields(
        _set(), field_source=FIELD_SOURCE_REALIZED_RECALIBRATION,
        recalibration_estimator=_RECALIBRATED)
    fragment = converted.fields["cycle-1"]
    assert fragment["duration"] == 1_250_000
    assert fragment["execution_cost"] == 7
    assert fragment["estimator"] == _RECALIBRATED.to_canonical_dict()
    assert converted.digest != contract_action_fields(_set()).digest


def test_an_unknown_field_source_refuses() -> None:
    with pytest.raises(MeasurementRefused) as excinfo:
        contract_action_fields(_set(), field_source="realized")
    assert excinfo.value.data["code"] == REFUSAL_UNKNOWN_FIELD_SOURCE


def test_converting_no_trace_set_refuses() -> None:
    with pytest.raises(MeasurementRefused):
        contract_action_fields([])


# --------------------------------------------------------------------------- #
# The conversion is validated by the FROZEN contract's own loader.             #
# --------------------------------------------------------------------------- #
def _instance_raw(fragments: dict[str, Any], cost_unit: str) -> dict[str, Any]:
    """A minimal batch instance whose actions take the measured fields."""
    actions = []
    for action_id, fragment in sorted(fragments.items()):
        action = {
            "id": action_id,
            "action_class": "measured_cycle",
            "covers": ["o1"],
            "requires": [],
            "conflicts": [],
            "reveals": {"obligation_classes": [], "action_classes": []},
            "effects": {"unknown": "the measurement states no effect summary"},
        }
        action.update(copy.deepcopy(fragment))
        actions.append(action)
    return {
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "instance_id": "measured-1",
        "epoch": 0,
        "cost_unit": cost_unit,
        "unit_mapping_version": "units/1",
        "changes": [{"id": "c1", "affected_scope": ["scope:a"]}],
        "obligations": [{
            "id": "o1",
            "obligation_class": "inspection",
            "priority": "should",
            "unit": {
                "spec_version": "spec/1",
                "subject_scope_key": "scope:a",
                "satisfaction_condition": "inspected",
            },
        }],
        "actions": actions,
        "conflict_groups": [],
        "resources": [
            {
                "id": resource,
                "version": 1,
                "segments": [{"start": 0, "capacity": 8}],
                "reservation": {"unknown": "no enforceable budget"},
                "hard_cap": {"unknown": "no enforceable cap"},
            }
            for resource in PARITY_RESOURCES
        ],
    }


def test_the_converted_fields_load_through_the_frozen_contract() -> None:
    converted = contract_action_fields(_set(_trace("a1"), _trace("a2")))
    instance = load_batch_instance(
        _instance_raw(dict(converted.fields), converted.cost_unit))
    assert {a.id for a in instance.actions} == {"a1", "a2"}
    for action in instance.actions:
        assert isinstance(action.duration, Unknown)
        assert isinstance(action.execution_cost, Unknown)
        assert set(action.demand) == set(PARITY_RESOURCES)
        assert action.estimator == ESTIMATOR_UNESTIMATED


def test_recalibrated_fields_also_load_through_the_frozen_contract() -> None:
    converted = contract_action_fields(
        _set(_trace("a1")), field_source=FIELD_SOURCE_REALIZED_RECALIBRATION,
        recalibration_estimator=_RECALIBRATED)
    instance = load_batch_instance(
        _instance_raw(dict(converted.fields), converted.cost_unit))
    action = instance.action_by_id("a1")
    assert action is not None
    assert action.duration == 1_250_000
    assert action.execution_cost == 7
    assert action.estimator == _RECALIBRATED


def test_a_foreign_cost_unit_is_refused_by_the_frozen_contract_too() -> None:
    """The measurement layer and the contract agree: unlike units refuse."""
    converted = contract_action_fields(_set(_trace("a1")))
    raw = _instance_raw(dict(converted.fields), "usd_cents")
    with pytest.raises(Exception) as excinfo:
        load_batch_instance(raw)
    assert excinfo.value.data["code"] == REFUSAL_MIXED_COST_UNITS


# --------------------------------------------------------------------------- #
# The parity adapter, exercised against the shape the real path returns.       #
# --------------------------------------------------------------------------- #
def test_a_cycle_trace_reads_the_ledgers_own_charged_work() -> None:
    execution = _FakeExecution()
    trace = cycle_trace_from_execution(
        execution, realized_duration=4_000, binding=_binding())
    assert trace.action_class == GRANULARITY_PUBLISH_CYCLE
    assert trace.realized.duration == 4_000
    assert trace.realized.cost == execution.ledger.steps_run
    assert trace.demand[PARITY_RESOURCE_EXECUTOR_STEPS] == 2
    assert trace.demand[PARITY_RESOURCE_CERTIFICATE_WRITES] == 1
    assert trace.demand[PARITY_RESOURCE_PUBLISH_COMMITS] == 1
    assert isinstance(trace.estimate.duration, Unknown)
    assert isinstance(trace.reservation, Unknown)
    assert trace.outcome == OUTCOME_SUCCESS


def test_an_undeclarable_resource_is_unknown_not_zero() -> None:
    trace = cycle_trace_from_execution(
        _FakeExecution(), realized_duration=4_000, binding=_binding(),
        declared_resources=(*PARITY_RESOURCES, "model_concurrency"),
    )
    unmeasured = trace.demand["model_concurrency"]
    assert isinstance(unmeasured, Unknown)
    assert "model_concurrency" in unmeasured.reason


def test_step_traces_cover_executions_and_never_skips() -> None:
    execution = _FakeExecution(
        run_steps=("s:a", "s:b", "s:a"),
        skip_outcomes=(("s:c", "certified"), ("s:d", "certified")),
    )
    traces = step_traces_from_execution(execution, binding=_binding())
    assert len(traces) == 3, "one trace per executed step OCCURRENCE"
    assert len({t.action_id for t in traces}) == 3
    for skipped in skipped_step_ids(execution):
        assert not [t for t in traces
                    if t.provenance["step_id"] == skipped], (
            f"skipped step {skipped!r} produced a realized execution trace"
        )
    for trace in traces:
        assert isinstance(trace.realized.duration, Unknown), (
            "a per-step duration is not measured; dividing the cycle "
            "interval would be a fabricated measurement"
        )
        assert trace.realized.cost == 1
        assert isinstance(
            trace.demand[PARITY_RESOURCE_CERTIFICATE_WRITES], Unknown)


def test_the_skip_cost_answer_is_recorded_explicitly() -> None:
    trace = cycle_trace_from_execution(
        _FakeExecution(), realized_duration=4_000, binding=_binding())
    assert "UNKNOWN rather than zero" in (
        trace.provenance["certificate_validation_cost"])
    assert trace.provenance["steps_skipped"] == "1"


def test_a_moved_execution_surface_refuses_rather_than_defaulting() -> None:
    class _Truncated:
        mode = "differential"
        index = 0
        run_steps = ()
        skip_outcomes = ()

    with pytest.raises(MeasurementRefused) as excinfo:
        cycle_trace_from_execution(
            _Truncated(), realized_duration=1, binding=_binding())
    assert excinfo.value.data["code"] == REFUSAL_ADAPTER_SURFACE


def test_a_non_fixed_point_terminal_is_outcome_unknown() -> None:
    execution = _FakeExecution(
        ledger=_FakeLedger(steps_run=1, steps_reached=1, terminal="stalled"))
    trace = cycle_trace_from_execution(
        execution, realized_duration=9, binding=_binding())
    assert trace.outcome != OUTCOME_SUCCESS


# --------------------------------------------------------------------------- #
# EWD-BMA-1: the outcome tracks the SCHEDULER's terminal vocabulary.           #
# --------------------------------------------------------------------------- #
def test_the_outcome_reads_the_schedulers_own_terminal_constant() -> None:
    """A restated literal here would drift silently.

    A mirrored ``"fixed_point"`` moves with neither the scheduler nor any
    wrapped-versus-plain comparison — both arms of such a comparison run
    the same drifted code — so a rename in ``reuse_scheduler`` would turn
    every genuinely successful pass into ``outcome: unknown`` with no red
    test anywhere, and every obligation those actions covered would
    discharge nothing. The adapter must therefore read the scheduler's
    own object, not a copy of its value."""
    assert (batch_measurements.TERMINAL_FIXED_POINT
            is reuse_scheduler.TERMINAL_FIXED_POINT), (
        "the adapter restated the scheduler's committing terminal instead "
        "of importing it; a drift in reuse_scheduler would silently "
        "degrade every measured outcome to UNKNOWN"
    )


def test_the_terminal_constant_is_imported_and_never_restated() -> None:
    """The value check above cannot see a same-valued mirror: equal string
    literals are interned, so ``is`` still holds the day the mirror is
    written. Only the module's own structure distinguishes "reads the
    scheduler's constant" from "happens to agree with it today", so assert
    the structure: imported from ``reuse_scheduler``, assigned nowhere."""
    source = Path(batch_measurements.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_from: set[str] = set()
    assigned: list[int] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if any(alias.name == "TERMINAL_FIXED_POINT"
                   for alias in node.names):
                imported_from.add(node.module or "")
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = (node.targets if isinstance(node, ast.Assign)
                       else [node.target])
            for target in targets:
                if (isinstance(target, ast.Name)
                        and target.id == "TERMINAL_FIXED_POINT"):
                    assigned.append(node.lineno)
    assert any(module.endswith("reuse_scheduler")
               for module in imported_from), (
        "TERMINAL_FIXED_POINT must be imported from reuse_scheduler"
    )
    assert not assigned, (
        f"TERMINAL_FIXED_POINT is restated at line(s) {assigned}; a mirror "
        "agrees with the scheduler only until the scheduler is renamed, and "
        "no wrapped-versus-plain witness can ever see that drift"
    )


@pytest.mark.parametrize("terminal,expect_success", [
    (reuse_scheduler.TERMINAL_FIXED_POINT, True),
    (reuse_scheduler.TERMINAL_DISCARD, False),
    (reuse_scheduler.TERMINAL_ABORT, False),
])
def test_every_scheduler_terminal_maps_to_a_stated_outcome(
    terminal: str, expect_success: bool
) -> None:
    """Built from the scheduler's constants, so the mapping is checked
    against the real vocabulary rather than against a fixture's copy."""
    execution = _FakeExecution(
        run_steps=("s:a",),
        ledger=_FakeLedger(steps_run=1, steps_reached=1, terminal=terminal))
    cycle = cycle_trace_from_execution(
        execution, realized_duration=9, binding=_binding())
    steps = step_traces_from_execution(execution, binding=_binding())
    assert (cycle.outcome == OUTCOME_SUCCESS) is expect_success
    assert all((t.outcome == OUTCOME_SUCCESS) is expect_success
               for t in steps)


# --------------------------------------------------------------------------- #
# EWD-BMA-2: the two granularity accounts may not be merged.                   #
# --------------------------------------------------------------------------- #
def _measured_one_update() -> Any:
    return measure_replay(
        _fake_replay, binding=_binding(), trace_set_id="witness",
        clock=_StepClock(),
        executions=[_FakeExecution(index=0, run_steps=("s:a", "s:b"))],
    )


def test_merging_the_two_granularity_accounts_would_double_charge() -> None:
    """The premise of the refusal below, measured rather than asserted."""
    measured = _measured_one_update()
    cycle_demand = sum(
        t.demand[PARITY_RESOURCE_EXECUTOR_STEPS]
        for t in measured.cycle_traces.traces)
    step_demand = sum(
        t.demand[PARITY_RESOURCE_EXECUTOR_STEPS]
        for t in measured.step_traces.traces)
    assert cycle_demand == 2, "the cycle already charges both steps"
    assert step_demand == 2, "the step account charges the same two steps"
    assert cycle_demand + step_demand == 4, (
        "summing the two accounts charges 4 executor steps for 2 executed "
        "steps; the checker multiplies demand by duration per resource, so "
        "a merged fragment set inflates every resource account"
    )


def test_the_pre_existing_cross_checks_cannot_see_the_merge() -> None:
    """Non-vacuity: unit and identity checks all PASS on this input, so
    only a granularity check can refuse it."""
    measured = _measured_one_update()
    cycles, steps = measured.cycle_traces, measured.step_traces
    assert cycles.cost_unit == steps.cost_unit
    assert cycles.duration_unit == steps.duration_unit
    assert not ({t.action_id for t in cycles.traces}
                & {t.action_id for t in steps.traces})
    assert cycles.granularity != steps.granularity


def test_converting_both_granularity_accounts_together_refuses() -> None:
    measured = _measured_one_update()
    with pytest.raises(MeasurementRefused) as excinfo:
        contract_action_fields([measured.cycle_traces, measured.step_traces])
    assert excinfo.value.data["code"] == REFUSAL_UNKNOWN_GRANULARITY


def test_each_granularity_account_converts_on_its_own() -> None:
    """The refusal is targeted: one account at a time still works."""
    measured = _measured_one_update()
    assert len(contract_action_fields(measured.cycle_traces).fields) == 1
    assert len(contract_action_fields(measured.step_traces).fields) == 2


# --------------------------------------------------------------------------- #
# EWD-BMA-3: the surface check tests SHAPE, not merely presence.               #
# --------------------------------------------------------------------------- #
class _AlwaysEqual:
    """A counterfeit terminal that answers ``True`` to any comparison."""

    def __eq__(self, other: Any) -> bool:
        return True

    def __hash__(self) -> int:
        return 0


def _counterfeit(**overrides: Any) -> _FakeExecution:
    """A well-shaped execution with one attribute RETYPED after the fact.

    Every counterfeit here passes ``hasattr``. That is the point: the
    realistic drift is an upstream field that becomes a ``float``, a
    ``Decimal``, or a ``numpy`` integer, not one that vanishes."""
    execution = _FakeExecution(index=0, run_steps=("s:a", "s:b"))
    for name, value in overrides.items():
        target = (execution.ledger if hasattr(execution.ledger, name)
                  else execution)
        setattr(target, name, value)
    return execution


@pytest.mark.parametrize("probe,overrides", [
    # C1: a string IS a Sequence, so this minted one trace per CHARACTER.
    ("run_steps_is_a_string", {"run_steps": "ab"}),
    # C2: `int()` truncated a float and recorded it as an exact count.
    ("certificates_written_is_a_float", {"certificates_written": 2.9}),
    # C3: `int()` coerced a string into a realized cost.
    ("steps_run_is_a_string", {"steps_run": "7"}),
    # C4: a falsey-MEANING string is truthy, so it charged a publish
    # commit that never happened.
    ("published_is_a_string", {"published": "false"}),
    # C5: the sharp one — a forged `outcome: success`, the only value
    # that discharges an obligation.
    ("terminal_compares_equal_to_anything", {"terminal": _AlwaysEqual()}),
])
def test_a_retyped_execution_surface_refuses_rather_than_coercing(
    probe: str, overrides: dict[str, Any]
) -> None:
    with pytest.raises(MeasurementRefused) as excinfo:
        cycle_trace_from_execution(
            _counterfeit(**overrides), realized_duration=1,
            binding=_binding())
    assert excinfo.value.data["code"] == REFUSAL_ADAPTER_SURFACE, probe


def test_a_forged_success_terminal_never_reaches_the_outcome() -> None:
    """C5 spelled out: the refusal is what stops the forgery, so state
    what the unrefused path would have produced."""
    forged = _AlwaysEqual()
    assert forged == reuse_scheduler.TERMINAL_FIXED_POINT, (
        "the probe is only meaningful if the counterfeit really would "
        "have compared equal to the committing terminal"
    )
    with pytest.raises(MeasurementRefused) as excinfo:
        cycle_trace_from_execution(
            _counterfeit(terminal=forged), realized_duration=1,
            binding=_binding())
    assert excinfo.value.data["code"] == REFUSAL_ADAPTER_SURFACE


@pytest.mark.parametrize("overrides", [
    {"run_steps": "ab"},
    {"steps_run": 3.0},
    {"terminal": _AlwaysEqual()},
    {"skip_outcomes": "ac"},
])
def test_the_step_reader_refuses_the_same_retyped_surfaces(
    overrides: dict[str, Any]
) -> None:
    with pytest.raises(MeasurementRefused) as excinfo:
        step_traces_from_execution(_counterfeit(**overrides),
                                   binding=_binding())
    assert excinfo.value.data["code"] == REFUSAL_ADAPTER_SURFACE


def test_a_retyped_skip_record_refuses_before_it_is_named() -> None:
    with pytest.raises(MeasurementRefused) as excinfo:
        skipped_step_ids(_counterfeit(skip_outcomes=("s:c",)))
    assert excinfo.value.data["code"] == REFUSAL_ADAPTER_SURFACE


@pytest.mark.parametrize("overrides", [
    {"mode": 7},
    {"index": 1.0},
    {"index": True},
    {"generation_id": None},
    {"fallback_cause": 12},
    {"steps_reached": "5"},
])
def test_every_other_consumed_attribute_is_typed_too(
    overrides: dict[str, Any]
) -> None:
    with pytest.raises(MeasurementRefused) as excinfo:
        cycle_trace_from_execution(_counterfeit(**overrides),
                                   realized_duration=1, binding=_binding())
    assert excinfo.value.data["code"] == REFUSAL_ADAPTER_SURFACE


def test_the_well_shaped_surface_still_passes() -> None:
    """The typed check must not refuse the real shape it was written for."""
    trace = cycle_trace_from_execution(
        _counterfeit(), realized_duration=1, binding=_binding())
    assert trace.outcome == OUTCOME_SUCCESS
    assert trace.realized.cost == 2


# --------------------------------------------------------------------------- #
# The clock is injectable, so a measurement is deterministic.                  #
# --------------------------------------------------------------------------- #
def _fake_replay(*, on_update: Callable[[Any], None],
                 executions: Sequence[Any]) -> dict[str, Any]:
    for execution in executions:
        on_update(execution)
    return {"executions": tuple(executions)}


def test_measure_replay_is_deterministic_under_an_injected_clock() -> None:
    executions = [_FakeExecution(index=i, run_steps=("s:a",))
                  for i in range(3)]
    digests = []
    for _ in range(2):
        measured = measure_replay(
            _fake_replay, binding=_binding(), trace_set_id="witness",
            clock=_StepClock(), executions=executions,
        )
        assert measured.run["executions"] == tuple(executions)
        assert [t.realized.duration for t in measured.cycle_traces.traces] == (
            [500, 500, 500])
        digests.append(measured.cycle_traces.digest)
    assert digests[0] == digests[1]


def test_measure_replay_chains_a_callers_observer() -> None:
    seen: list[int] = []
    executions = [_FakeExecution(index=i, run_steps=("s:a",))
                  for i in range(2)]
    measure_replay(
        _fake_replay, binding=_binding(), trace_set_id="witness",
        clock=_StepClock(), on_update=lambda e: seen.append(e.index),
        executions=executions,
    )
    assert seen == [0, 1], "the caller's own observer must still run"


def test_measure_replay_keeps_the_two_granularities_apart() -> None:
    executions = [_FakeExecution(index=0, run_steps=("s:a", "s:b"))]
    measured = measure_replay(
        _fake_replay, binding=_binding(), trace_set_id="witness",
        clock=_StepClock(), executions=executions,
    )
    assert measured.cycle_traces.granularity == GRANULARITY_PUBLISH_CYCLE
    assert measured.step_traces.granularity == GRANULARITY_EXECUTOR_STEP
    assert len(measured.cycle_traces.traces) == 1
    assert len(measured.step_traces.traces) == 2
    assert measured.cycle_traces.digest != measured.step_traces.digest


# --------------------------------------------------------------------------- #
# EWD-BMA-4: the first interval is a DIFFERENT instrument, and says so.        #
# --------------------------------------------------------------------------- #
def _measured_three_updates() -> Any:
    executions = [_FakeExecution(index=i, run_steps=("s:a",))
                  for i in range(3)]
    return measure_replay(
        _fake_replay, binding=_binding(), trace_set_id="witness",
        clock=_StepClock(), executions=executions,
    )


def test_the_first_interval_declares_the_setup_it_includes() -> None:
    """The clock is marked before the driver runs, and the driver's
    one-time setup (for ``parity.replay_mode``, materializing the whole
    history's base corpus) lands inside the first interval. That term
    scales with history size, so one label covering both instruments
    would make a cycle-0 comparison across histories false."""
    measured = _measured_three_updates()
    sources = [t.realized.measurement_source
               for t in sorted(measured.cycle_traces.traces,
                               key=lambda t: t.action_id)]
    assert sources[0] == MEASUREMENT_SOURCE_CYCLE_INTERVAL_WITH_SETUP
    assert sources[1:] == [MEASUREMENT_SOURCE_CYCLE_INTERVAL] * 2
    assert sources[0] != sources[1], (
        "two intervals measured by different instruments must not share "
        "one label"
    )
    assert "setup" in sources[0], (
        "the label must NAME the extra term, not merely differ"
    )


def test_recalibrating_from_the_setup_charged_interval_refuses() -> None:
    measured = _measured_three_updates()
    with pytest.raises(MeasurementRefused) as excinfo:
        contract_action_fields(
            measured.cycle_traces,
            field_source=FIELD_SOURCE_REALIZED_RECALIBRATION,
            recalibration_estimator=_RECALIBRATED)
    assert excinfo.value.data["code"] == REFUSAL_SETUP_CHARGED_REALIZED
    assert excinfo.value.data["measurement_source"] == (
        MEASUREMENT_SOURCE_CYCLE_INTERVAL_WITH_SETUP)


def test_the_setup_charged_interval_is_admitted_only_when_asked() -> None:
    measured = _measured_three_updates()
    converted = contract_action_fields(
        measured.cycle_traces,
        field_source=FIELD_SOURCE_REALIZED_RECALIBRATION,
        recalibration_estimator=_RECALIBRATED,
        include_setup_charged_realized=True)
    assert len(converted.fields) == 3


def test_the_default_estimate_conversion_is_unaffected_by_the_label() -> None:
    """The estimate source reads no realized number, so the setup-charged
    instrument cannot reach it either way."""
    measured = _measured_three_updates()
    converted = contract_action_fields(measured.cycle_traces)
    assert len(converted.fields) == 3
    assert all(f["duration"]["unknown"] for f in converted.fields.values())


# --------------------------------------------------------------------------- #
# EWD-BMA-5: the conversion result validates like every other dataclass.       #
# --------------------------------------------------------------------------- #
def _fragment_for(action_id: str = "a1") -> dict[str, Any]:
    return dict(contract_action_fields(_set(_trace(action_id)))
                .fields[action_id])


def test_a_hand_built_conversion_refuses_an_arbitrary_field_source() -> None:
    with pytest.raises(MeasurementRefused) as excinfo:
        ContractActionFields(
            cost_unit=COST_UNIT_EXECUTOR_STEPS,
            duration_unit=DURATION_UNIT_NANOSECONDS,
            field_source="realized", fields={"a1": _fragment_for()})
    assert excinfo.value.data["code"] == REFUSAL_UNKNOWN_FIELD_SOURCE


def test_a_hand_built_conversion_refuses_an_empty_field_map() -> None:
    with pytest.raises(MeasurementRefused) as excinfo:
        ContractActionFields(
            cost_unit=COST_UNIT_EXECUTOR_STEPS,
            duration_unit=DURATION_UNIT_NANOSECONDS,
            field_source=FIELD_SOURCE_ESTIMATE, fields={})
    assert excinfo.value.data["code"] == REFUSAL_MISSING_FIELD


def test_a_hand_built_conversion_refuses_a_foreign_fragment_unit() -> None:
    fragment = _fragment_for()
    fragment["cost_unit"] = "usd_cents"
    with pytest.raises(MeasurementRefused) as excinfo:
        ContractActionFields(
            cost_unit=COST_UNIT_EXECUTOR_STEPS,
            duration_unit=DURATION_UNIT_NANOSECONDS,
            field_source=FIELD_SOURCE_ESTIMATE, fields={"a1": fragment})
    assert excinfo.value.data["code"] == REFUSAL_MIXED_COST_UNITS


def test_a_hand_built_conversion_refuses_a_zero_length_action() -> None:
    fragment = _fragment_for()
    fragment["duration"] = 0
    with pytest.raises(MeasurementRefused) as excinfo:
        ContractActionFields(
            cost_unit=COST_UNIT_EXECUTOR_STEPS,
            duration_unit=DURATION_UNIT_NANOSECONDS,
            field_source=FIELD_SOURCE_ESTIMATE, fields={"a1": fragment})
    assert excinfo.value.data["code"] == REFUSAL_NON_CANONICAL_VALUE


def test_a_hand_built_conversion_refuses_an_invented_fragment_key() -> None:
    fragment = _fragment_for()
    fragment["realized_duration_ms"] = 12
    with pytest.raises(MeasurementRefused) as excinfo:
        ContractActionFields(
            cost_unit=COST_UNIT_EXECUTOR_STEPS,
            duration_unit=DURATION_UNIT_NANOSECONDS,
            field_source=FIELD_SOURCE_ESTIMATE, fields={"a1": fragment})
    assert excinfo.value.data["code"] == REFUSAL_UNKNOWN_FIELD


# --------------------------------------------------------------------------- #
# EWD-BMA-6: the §12 resource binding must MOVE with the configuration.        #
# --------------------------------------------------------------------------- #
def test_the_resource_binding_varies_with_the_declaration() -> None:
    """A digest that varies with nothing satisfies §12 in presence and is
    empty in meaning: presence is not coverage."""
    base = resource_declaration_digest(PARITY_RESOURCES)
    assert base != resource_declaration_digest(
        (*PARITY_RESOURCES, "model_concurrency"))
    assert base != resource_declaration_digest(
        PARITY_RESOURCES, cost_unit="model_tokens")
    assert base != resource_declaration_digest(
        PARITY_RESOURCES, capacities={PARITY_RESOURCE_EXECUTOR_STEPS: 8})
    assert base == resource_declaration_digest(tuple(reversed(
        PARITY_RESOURCES))), "declaration ORDER is not a configuration change"


def test_the_resource_binding_refuses_an_empty_declaration() -> None:
    with pytest.raises(MeasurementRefused) as excinfo:
        resource_declaration_digest(())
    assert excinfo.value.data["code"] == REFUSAL_MISSING_FIELD


def test_a_capacity_for_an_undeclared_resource_refuses() -> None:
    with pytest.raises(MeasurementRefused) as excinfo:
        resource_declaration_digest(
            PARITY_RESOURCES, capacities={"model_concurrency": 4})
    assert excinfo.value.data["code"] == REFUSAL_UNDECLARED_RESOURCE


# --------------------------------------------------------------------------- #
# The §12 repo binding.                                                        #
# --------------------------------------------------------------------------- #
def test_the_repo_binding_reads_the_injected_git_state() -> None:
    calls: list[tuple[str, ...]] = []

    def _git(argv: Sequence[str]) -> str:
        calls.append(tuple(argv))
        return "abc1234\n" if argv[0] == "rev-parse" else " M src/x.py\n"

    binding = okg_repo_binding(
        repo_root="/repo", corpus_manifest_digest="m" * 64,
        resource_config_version=1, resource_config_digest="r" * 64,
        machine_runtime_label="test-machine", run_git=_git,
    )
    assert binding.okg_commit == "abc1234"
    assert binding.okg_tree_status == TREE_DIRTY
    assert calls == [("rev-parse", "HEAD"), ("status", "--porcelain")]


def test_a_clean_tree_binds_clean() -> None:
    binding = okg_repo_binding(
        repo_root="/repo", corpus_manifest_digest="m" * 64,
        resource_config_version=1, resource_config_digest="r" * 64,
        run_git=lambda argv: "abc1234\n" if argv[0] == "rev-parse" else "",
    )
    assert binding.okg_tree_status == TREE_CLEAN
    assert binding.machine_runtime_label


def test_a_missing_head_refuses_rather_than_guessing() -> None:
    with pytest.raises(MeasurementRefused) as excinfo:
        okg_repo_binding(
            repo_root="/repo", corpus_manifest_digest="m" * 64,
            resource_config_version=1, resource_config_digest="r" * 64,
            run_git=lambda argv: "",
        )
    assert excinfo.value.data["code"] == REFUSAL_ADAPTER_SURFACE


# --------------------------------------------------------------------------- #
# The DB witness: the live parity path, wrapped, with semantics unchanged.     #
# --------------------------------------------------------------------------- #
psycopg = pytest.importorskip("psycopg")

from okg.bench.synth.parity import (  # noqa: E402
    MODE_COMPLETE,
    MODE_DIFFERENTIAL,
    VERDICT_CERTIFIED,
    VERDICT_EQUAL,
    grade_update,
    replay_mode,
)
from tests.bench.synth.conftest import (  # noqa: E402,F401  (fixture re-export)
    clean_runtime,
    okg_dsn,
    okg_migrated,
    provenance,
    smoke_config,
    world,
)
from tests.bench.synth.test_parity_replay import (  # noqa: E402
    _DEP_DIR,
    _install_composed_catalog,
    _reset_between_modes,
    _seal_recorded_history,
)

#: Short on purpose: the standing 22-update parity witness already exists;
#: this witness needs the measurement wrapper, not a second marathon.
_SMOKE_UPDATES = 6

_REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.mark.postgres
def test_the_wrapped_live_replay_measures_without_changing_semantics(
    clean_runtime: str, tmp_path: Path, world, smoke_config  # noqa: F811
) -> None:
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)
    parity, policies, checks, _direct = _seal_recorded_history(
        dsn, tmp_path, world, smoke_config, _SMOKE_UPDATES
    )
    limit = min(_SMOKE_UPDATES, len(parity.updates))

    _reset_between_modes(dsn)
    reference = replay_mode(
        dsn=dsn, deployment_dir=_DEP_DIR, corpus_dir=tmp_path / "corpus-ref",
        parity=parity, mode=MODE_COMPLETE, policies=policies, checks=checks,
        limit=limit,
    )

    # The UNWRAPPED differential run: the control.
    _reset_between_modes(dsn)
    plain = replay_mode(
        dsn=dsn, deployment_dir=_DEP_DIR, corpus_dir=tmp_path / "corpus-plain",
        parity=parity, mode=MODE_DIFFERENTIAL, policies=policies,
        checks=checks, limit=limit,
    )

    # The WRAPPED differential run: same driver, same arguments, plus the
    # adapter's observer and clock.
    # The §12 resource binding is the digest of the resource declaration
    # this run actually used. An empty trace set's digest would be a
    # constant that varies with no capacity, cap, or resource name, which
    # is presence without coverage.
    resource_digest = resource_declaration_digest(
        PARITY_RESOURCES,
        cost_unit=COST_UNIT_EXECUTOR_STEPS,
        duration_unit=DURATION_UNIT_NANOSECONDS,
    )
    assert resource_digest != resource_declaration_digest(
        (*PARITY_RESOURCES, "model_concurrency")), (
        "the bound resource digest must move when the declaration moves"
    )
    binding = okg_repo_binding(
        repo_root=_REPO_ROOT,
        corpus_manifest_digest=parity.updates[limit - 1].digest,
        resource_config_version=1,
        resource_config_digest=resource_digest,
    )
    _reset_between_modes(dsn)
    measured = measure_replay(
        replay_mode, binding=binding, trace_set_id="parity-smoke",
        dsn=dsn, deployment_dir=_DEP_DIR,
        corpus_dir=tmp_path / "corpus-measured", parity=parity,
        mode=MODE_DIFFERENTIAL, policies=policies, checks=checks, limit=limit,
    )

    # --- semantics unchanged: asserted, not assumed ----------------------- #
    assert len(measured.run.executions) == len(plain.executions)
    for wrapped_exec, plain_exec in zip(measured.run.executions,
                                        plain.executions, strict=True):
        # ``generation_id`` carries the publish timestamp, so it differs
        # between any two runs of the same history; every other charged
        # quantity must be identical.
        wrapped_row = wrapped_exec.ledger.to_json_dict()
        plain_row = plain_exec.ledger.to_json_dict()
        wrapped_row.pop("generation_id")
        plain_row.pop("generation_id")
        assert wrapped_row == plain_row
        assert wrapped_exec.run_steps == plain_exec.run_steps
        assert wrapped_exec.skip_outcomes == plain_exec.skip_outcomes
        assert wrapped_exec.certificate_digests == \
            plain_exec.certificate_digests
    for index in range(limit):
        wrapped_verdict = grade_update(
            gold_step=parity.gold_step(index),
            reference=reference.executions[index],
            differential=measured.run.executions[index],
        )
        plain_verdict = grade_update(
            gold_step=parity.gold_step(index),
            reference=reference.executions[index],
            differential=plain.executions[index],
        )
        assert wrapped_verdict.verdict == plain_verdict.verdict, (
            f"update {index}: wrapping changed the parity verdict "
            f"({plain_verdict.verdict} -> {wrapped_verdict.verdict})"
        )
        assert wrapped_verdict.verdict in (VERDICT_EQUAL, VERDICT_CERTIFIED)

    # --- one realized trace per executed action --------------------------- #
    cycles = measured.cycle_traces
    steps = measured.step_traces
    assert len(cycles.traces) == limit
    for trace in cycles.traces:
        assert isinstance(trace.realized.duration, int)
        assert trace.realized.duration > 0, (
            "a live publish cycle that measured zero nanoseconds is an "
            "unresolved interval, not a free one"
        )
        assert set(trace.demand) == set(PARITY_RESOURCES)

    expected_steps = sum(len(e.run_steps) for e in measured.run.executions)
    assert len(steps.traces) == expected_steps
    assert len({t.action_id for t in steps.traces}) == expected_steps

    # --- a skipped step produces NO realized execution trace -------------- #
    skipped = {
        (execution.index, step)
        for execution in measured.run.executions
        for step in skipped_step_ids(execution)
    }
    assert skipped, (
        "the differential mode skipped nothing over this history, so the "
        "no-trace-for-a-skip claim would be vacuous"
    )
    traced = {(int(t.provenance["update"]), t.provenance["step_id"])
              for t in steps.traces}
    assert not (skipped & traced), sorted(skipped & traced)[:3]

    # --- the first cycle's instrument is DISTINCT and says what it adds --- #
    # ``replay_mode`` materializes the whole-history base corpus once,
    # before its first ``on_update`` mark, so trace 0's interval carries
    # setup that no later interval does. One label over two instruments
    # would make a cycle-0 comparison across histories false.
    by_id = sorted(cycles.traces, key=lambda t: t.action_id)
    assert by_id[0].realized.measurement_source == \
        MEASUREMENT_SOURCE_CYCLE_INTERVAL_WITH_SETUP
    assert {t.realized.measurement_source for t in by_id[1:]} == \
        {MEASUREMENT_SOURCE_CYCLE_INTERVAL}
    assert "setup" in by_id[0].realized.measurement_source
    with pytest.raises(MeasurementRefused) as setup_refusal:
        contract_action_fields(
            cycles, field_source=FIELD_SOURCE_REALIZED_RECALIBRATION,
            recalibration_estimator=_RECALIBRATED)
    assert setup_refusal.value.data["code"] == REFUSAL_SETUP_CHARGED_REALIZED

    # --- the §12 bindings match the LIVE repository ----------------------- #
    # Read independently of ``okg_repo_binding``: comparing that function
    # against itself proves only that it is deterministic, never that
    # either value is the repository's actual state.
    def _git(*argv: str) -> str:
        return subprocess.run(
            ["git", "-C", str(_REPO_ROOT), *argv],
            capture_output=True, text=True, check=True,
        ).stdout

    head = _git("rev-parse", "HEAD").strip()
    dirty = bool(_git("status", "--porcelain").strip())
    assert len(head) == 40 and head != "0" * 40
    for trace in (*cycles.traces, *steps.traces):
        assert trace.binding.okg_commit == head
        assert trace.binding.okg_tree_status == (
            TREE_DIRTY if dirty else TREE_CLEAN)
        assert trace.binding.corpus_manifest_digest == \
            parity.updates[limit - 1].digest
        assert trace.binding.resource_config_digest == resource_digest

    # --- and the traces convert into the frozen contract ------------------ #
    converted = contract_action_fields(cycles)
    instance = load_batch_instance(
        _instance_raw(dict(converted.fields), converted.cost_unit))
    assert len(instance.actions) == limit
    for action in instance.actions:
        assert isinstance(action.duration, Unknown), (
            "the contract's duration field is an ESTIMATE; this path has no "
            "estimator, so a measured run must not fill it"
        )

    # round-tripping the live traces keeps their digest
    assert load_trace_set(cycles.to_canonical_dict()).digest == cycles.digest
    assert replace(cycles, trace_set_id="other").digest != cycles.digest
