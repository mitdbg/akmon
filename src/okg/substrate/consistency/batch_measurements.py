"""Per-action measurement traces around the EXISTING exact execution paths.

PACT ``concord-disclosure-budget``, task.batch.measurement-adapter
(req.batch.checker-implementation), step 3 of the bounded implementation
sequence in ``designs/batch-scheduling-research.md`` §13: *"Add per-action
measurement adapters around existing exact execution. Do not replace the
reuse certificate or fixed-point semantics."* Normative sections: §7.1
(cost semantics), §7 (named resource demand, UNKNOWN never zero), §12
(reproducibility bindings), §4.3 (the named gap this closes).

The gap being closed
--------------------
§4.3 records it exactly: *"current synthetic ledgers contain counts and
causes, and current run bindings contain whole-stage latency and data
digests, but neither supplies the full scheduling trace contract."*
:class:`...parity.LedgerRow` counts runs, skips, causes, and certificates
but carries no duration, no estimate, no resource demand, and no commit
binding; :class:`...telemetry.work_ledger.WorkLedgerRow` charges six work
categories per ``(generation, mode, category)`` — a whole cycle, coarser
than one action, and with no estimate/realized distinction because it has
no estimates at all. This module supplies the missing per-action record.

What it does NOT do
-------------------
It builds NO estimator. §13 defers that to later slices, so every
estimate this module produces from today's execution paths is
:class:`...batch_schedule.Unknown` with a named reason, carried under
:data:`ESTIMATOR_UNESTIMATED`. Fabricating an estimate here — even a
"reasonable" one derived from the realized value — would silently make
the objective optimal with respect to a number nobody reviewed. §7.1 is
explicit that the objective is optimal *only* with respect to the pinned
declared estimates, so an unreviewed estimate is worse than no estimate.

It also changes NOTHING it measures. There is no import of
:mod:`okg.bench` here and no patch to any execution module: the parity
adapter is structural duck typing over the objects
:func:`...parity.run_update` ALREADY returns, and the wall clock is taken
in the caller's loop through the ``on_update`` observation hook
:func:`...parity.replay_mode` already offers. Fixed-point, certificate,
fallback, and repair-selection semantics are untouched because no code on
those paths is executed differently, or at all, by this module.

The three quantities §7.1 requires kept apart
---------------------------------------------
* :class:`PlanEstimate` — what planning used, with estimator identity and
  version. It carries no ``measurement_source``; the loader refuses that
  key by name with :data:`REFUSAL_REALIZED_AS_ESTIMATE` rather than as a
  generic unknown field, because presenting a measurement as an estimate
  is the specific dishonesty §7.1 exists to prevent.
* :class:`Reservation` — an enforceable cap or budget. §7 admits a bound
  only where something can *actually* enforce it, so
  :attr:`Reservation.enforced_by` must name the enforcing mechanism, and
  a path with no admission control states :data:`UNKNOWN` instead of a
  cap nothing checks.
* :class:`RealizedMeasurement` — what actually happened, with the
  measuring instrument named. It carries no ``estimator``; that key is
  refused by name for the mirror-image reason.

Because they are three distinct types on three distinct fields, no
consumer can read one for another, and the separation survives
serialization: the wire form of a trace has three separate objects.

Fail-closed rules inherited from the frozen contract
----------------------------------------------------
:mod:`.batch_schedule` is the trusted base. This module imports its
:func:`...canonical_json` / :func:`...object_digest` rather than
re-deriving canonicalization, so a trace digests through exactly the
bytes the contract does — integers and strings only, no floats, no
nulls, no booleans — and inherits:

* **UNKNOWN is a type, never a zero.** An unmeasured duration, an
  unattributable resource, an absent estimate: all
  :class:`...batch_schedule.Unknown` with a reason.
* **Silence is not an answer.** A trace answers for EVERY resource its
  set declares. A missing key refuses with
  :data:`REFUSAL_MISSING_RESOURCE_DEMAND`; an undeclared key refuses with
  :data:`REFUSAL_UNDECLARED_RESOURCE`. Zero is written, never implied.
* **One declared scalar unit.** A trace's ``cost_unit`` must equal its
  set's, and a conversion spanning sets with unlike units REFUSES
  (§7.1: unlike units may not be added).
* **A zero-length measurement is not free, it is unresolved.** A realized
  duration of ``0`` refuses: it means the clock could not resolve the
  interval, which is UNKNOWN. This matches the frozen contract's refusal
  of ``duration: 0`` on an action.

Granularity is declared, never mixed
------------------------------------
A trace set declares ONE :data:`GRANULARITIES` value and every trace's
``action_class`` must equal it. The parity adapter therefore returns TWO
sets — one publish-cycle account and one executor-step account — instead
of one mixed bag, because the same work appears in both and summing
across them would double-charge it. The same rule holds at the
conversion boundary: :func:`contract_action_fields` pins the granularity
of the first set it is given and REFUSES a set at the other granularity
with :data:`REFUSAL_UNKNOWN_GRANULARITY`, because a merged fragment set
charges a resource twice for one execution downstream.
"""
from __future__ import annotations

import platform
import subprocess
import sys
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping, Sequence

from okg.substrate.consistency.batch_schedule import (
    CONTRACT_SCHEMA_VERSION,
    OUTCOME_SUCCESS,
    OUTCOME_UNKNOWN,
    OUTCOMES,
    REFUSAL_DUPLICATE_IDENTITY,
    REFUSAL_MISSING_FIELD,
    REFUSAL_MIXED_COST_UNITS,
    REFUSAL_NON_CANONICAL_VALUE,
    REFUSAL_SCHEMA_VERSION,
    REFUSAL_UNKNOWN_FIELD,
    EstimatorRef,
    MaybeInt,
    Unknown,
    canonical_json,
    object_digest,
)
from okg.substrate.consistency.reuse_scheduler import TERMINAL_FIXED_POINT
from okg.substrate.errors import SubstrateError

__all__ = [
    "ActionTrace",
    "ActionTraceSet",
    "COST_UNIT_EXECUTOR_STEPS",
    "ContractActionFields",
    "DURATION_UNIT_NANOSECONDS",
    "ESTIMATOR_UNESTIMATED",
    "FIELD_SOURCES",
    "FIELD_SOURCE_ESTIMATE",
    "FIELD_SOURCE_REALIZED_RECALIBRATION",
    "GRANULARITIES",
    "GRANULARITY_EXECUTOR_STEP",
    "GRANULARITY_PUBLISH_CYCLE",
    "MEASUREMENT_SCHEMA_VERSION",
    "MEASUREMENT_SOURCE_CYCLE_INTERVAL",
    "MEASUREMENT_SOURCE_CYCLE_INTERVAL_WITH_SETUP",
    "MEASUREMENT_SOURCE_LEDGER",
    "MeasuredReplay",
    "MeasurementRefused",
    "PARITY_RESOURCES",
    "PARITY_RESOURCE_CERTIFICATE_WRITES",
    "PARITY_RESOURCE_EXECUTOR_STEPS",
    "PARITY_RESOURCE_PUBLISH_COMMITS",
    "PlanEstimate",
    "REFUSAL_ADAPTER_SURFACE",
    "REFUSAL_CODES",
    "REFUSAL_ESTIMATOR_DISAGREEMENT",
    "REFUSAL_MISSING_RESOURCE_DEMAND",
    "REFUSAL_MIXED_DURATION_UNITS",
    "REFUSAL_REALIZED_AS_ESTIMATE",
    "REFUSAL_SETUP_CHARGED_REALIZED",
    "REFUSAL_UNDECLARED_RESOURCE",
    "REFUSAL_UNKNOWN_FIELD_SOURCE",
    "REFUSAL_UNKNOWN_GRANULARITY",
    "REFUSAL_UNMEASURED_ZERO",
    "REFUSAL_UNSUPPORTED_OUTCOME",
    "RealizedMeasurement",
    "ReplayTimer",
    "ReproducibilityBinding",
    "Reservation",
    "SETUP_CHARGED_MEASUREMENT_SOURCES",
    "TERMINAL_FIXED_POINT",
    "TREE_CLEAN",
    "TREE_DIRTY",
    "TREE_STATUSES",
    "contract_action_fields",
    "cycle_trace_from_execution",
    "default_machine_runtime_label",
    "load_trace_set",
    "measure_replay",
    "okg_repo_binding",
    "resource_declaration_digest",
    "skipped_step_ids",
    "step_traces_from_execution",
    "trace_set_digest",
]


#: The measurement schema version. It binds the frozen contract version it
#: converts into, so a contract change is a trace-digest change (§12).
MEASUREMENT_SCHEMA_VERSION = "batch-measurement/1"

#: The realized-duration base unit. Integers only, so a digest is
#: byte-stable and no float ever enters the canonical form.
DURATION_UNIT_NANOSECONDS = "nanoseconds"

#: The declared scalar cost unit of the parity execution path: executor
#: steps actually charged by the reuse pass. It is operational work, not
#: harm, severity, or willingness to pay (§7.1).
COST_UNIT_EXECUTOR_STEPS = "executor_steps"

GRANULARITY_PUBLISH_CYCLE = "publish_cycle"
GRANULARITY_EXECUTOR_STEP = "executor_step"
#: A trace set declares exactly one. The same work appears at both
#: granularities, so the two accounts are never merged.
GRANULARITIES = (GRANULARITY_EXECUTOR_STEP, GRANULARITY_PUBLISH_CYCLE)

TREE_CLEAN = "clean"
TREE_DIRTY = "dirty"
#: §12 binds "OKG commit and dirty-tree status". Spelled as a closed
#: string vocabulary because the canonical form admits no booleans.
TREE_STATUSES = (TREE_CLEAN, TREE_DIRTY)

FIELD_SOURCE_ESTIMATE = "estimate"
FIELD_SOURCE_REALIZED_RECALIBRATION = "realized_recalibration"
#: Which of the three §7.1 quantities a contract conversion draws from.
#: The recalibration source exists so that feeding realized measurements
#: back as next-epoch estimates is a NAMED, digest-visible act with its
#: own estimator identity — never an implicit substitution.
FIELD_SOURCES = (FIELD_SOURCE_ESTIMATE, FIELD_SOURCE_REALIZED_RECALIBRATION)

#: The estimator identity meaning "no estimator has been built yet".
#: §13 defers the estimator to a later slice; until then every estimate
#: an adapter produces is UNKNOWN under this identity, and a trace that
#: carries this identity with a stated number REFUSES.
ESTIMATOR_UNESTIMATED = EstimatorRef(id="concord.batch.unestimated", version=1)

#: The instrument names a realized measurement may declare. Naming the
#: instrument is what lets a reader judge what the number includes.
MEASUREMENT_SOURCE_CYCLE_INTERVAL = (
    "monotonic_ns_interval_between_replay_loop_marks"
    ":includes_per_update_corpus_materialization"
)
#: The FIRST interval of a replay is a different instrument from every
#: later one. ``parity.replay_mode`` materializes the whole-history base
#: corpus once, before its first ``on_update`` mark, so the opening
#: interval carries that one-time setup on top of the per-update terms.
#: The quantity scales with history size, so a reader who applied
#: :data:`MEASUREMENT_SOURCE_CYCLE_INTERVAL` to it would attribute
#: setup to scheduler work and would compare two histories unfairly.
#: One label may not cover two instruments, so the first interval
#: declares its own and names the extra term.
MEASUREMENT_SOURCE_CYCLE_INTERVAL_WITH_SETUP = (
    "monotonic_ns_interval_between_replay_loop_marks"
    ":includes_per_update_corpus_materialization"
    ":includes_one_time_driver_setup_before_the_first_update"
)
MEASUREMENT_SOURCE_LEDGER = "parity_ledger_row_counts"

#: Instruments whose realized number includes work that is NOT the
#: measured action's own. Such a number is a true elapsed interval, so it
#: is reportable — but reading it back as a per-action estimate would
#: charge setup to the action, so the conversion refuses it unless the
#: caller asks for it by name.
SETUP_CHARGED_MEASUREMENT_SOURCES = (
    MEASUREMENT_SOURCE_CYCLE_INTERVAL_WITH_SETUP,
)

PARITY_RESOURCE_EXECUTOR_STEPS = "executor_steps"
PARITY_RESOURCE_CERTIFICATE_WRITES = "certificate_writes"
PARITY_RESOURCE_PUBLISH_COMMITS = "publish_commits"
#: The named resources the parity ledger can actually answer for. A
#: caller may declare others; the adapter then answers UNKNOWN with a
#: reason for them rather than writing an unearned zero.
PARITY_RESOURCES = (
    PARITY_RESOURCE_CERTIFICATE_WRITES,
    PARITY_RESOURCE_EXECUTOR_STEPS,
    PARITY_RESOURCE_PUBLISH_COMMITS,
)


# --------------------------------------------------------------------------- #
# Refusal vocabulary. Shared codes keep the frozen contract's spelling.        #
# --------------------------------------------------------------------------- #
REFUSAL_MIXED_DURATION_UNITS = "mixed_duration_units"
REFUSAL_UNDECLARED_RESOURCE = "undeclared_resource"
REFUSAL_MISSING_RESOURCE_DEMAND = "missing_resource_demand"
REFUSAL_REALIZED_AS_ESTIMATE = "realized_as_estimate"
REFUSAL_UNKNOWN_GRANULARITY = "unknown_granularity"
REFUSAL_UNKNOWN_FIELD_SOURCE = "unknown_field_source"
REFUSAL_ESTIMATOR_DISAGREEMENT = "estimator_disagreement"
REFUSAL_UNMEASURED_ZERO = "unmeasured_zero"
REFUSAL_UNSUPPORTED_OUTCOME = "unsupported_outcome"
REFUSAL_ADAPTER_SURFACE = "adapter_surface_missing"
REFUSAL_SETUP_CHARGED_REALIZED = "setup_charged_realized"

REFUSAL_CODES = (
    REFUSAL_ADAPTER_SURFACE,
    REFUSAL_SETUP_CHARGED_REALIZED,
    REFUSAL_DUPLICATE_IDENTITY,
    REFUSAL_ESTIMATOR_DISAGREEMENT,
    REFUSAL_MISSING_FIELD,
    REFUSAL_MISSING_RESOURCE_DEMAND,
    REFUSAL_MIXED_COST_UNITS,
    REFUSAL_MIXED_DURATION_UNITS,
    REFUSAL_NON_CANONICAL_VALUE,
    REFUSAL_REALIZED_AS_ESTIMATE,
    REFUSAL_SCHEMA_VERSION,
    REFUSAL_UNDECLARED_RESOURCE,
    REFUSAL_UNKNOWN_FIELD,
    REFUSAL_UNKNOWN_FIELD_SOURCE,
    REFUSAL_UNKNOWN_GRANULARITY,
    REFUSAL_UNMEASURED_ZERO,
    REFUSAL_UNSUPPORTED_OUTCOME,
)


class MeasurementRefused(SubstrateError):
    """A trace, trace set, or conversion failed the fail-closed check.

    ``data['code']`` is one of :data:`REFUSAL_CODES`; ``data['where']``
    names the offending object path. A distinct exception type from
    :class:`...batch_schedule.BatchLoadRefused` on purpose: a caller must
    be able to tell "the contract refused this instance" from "the
    measurement layer refused this trace"."""

    code = "batch_measurement_refused"


def _refuse(code: str, where: str, message: str, **extra: Any) -> None:
    raise MeasurementRefused(
        f"batch measurement refused [{code}] at {where}: {message}",
        data={"code": code, "where": where, **extra},
    )


def _quantity(value: Any) -> Any:
    """Canonical rendering of a :data:`...batch_schedule.MaybeInt`."""
    if isinstance(value, Unknown):
        return value.to_canonical_dict()
    return value


def _require_maybe_int(value: Any, where: str, *, minimum: int = 0) -> MaybeInt:
    if isinstance(value, Unknown):
        if not value.reason:
            _refuse(REFUSAL_NON_CANONICAL_VALUE, where,
                    "an UNKNOWN must state a non-empty reason")
        return value
    if isinstance(value, bool) or not isinstance(value, int):
        _refuse(
            REFUSAL_NON_CANONICAL_VALUE, where,
            "expected an integer in the declared base unit, or the "
            "UNKNOWN sentinel; an absent quantity is never zero",
        )
    if value < minimum:
        _refuse(REFUSAL_NON_CANONICAL_VALUE, where,
                f"expected an integer >= {minimum}")
    return value


def _require_str(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value:
        _refuse(REFUSAL_NON_CANONICAL_VALUE, where,
                "expected a non-empty string")
    return value


def _require_choice(value: Any, where: str, choices: Sequence[str],
                    code: str) -> str:
    value = _require_str(value, where)
    if value not in choices:
        _refuse(code, where,
                f"expected one of {tuple(choices)!r}, got {value!r}")
    return value


# --------------------------------------------------------------------------- #
# §12 reproducibility bindings.                                                #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ReproducibilityBinding:
    """What every trace binds so a measurement can be replayed (§12).

    A measurement with no binding is a number with no provenance: it
    cannot be attributed to a code state, a corpus, a resource
    configuration, or a machine, so it can neither be reproduced nor
    fairly compared with another run. Every field is digested, so
    changing any of them changes the trace's identity — a run on a dirty
    tree can never be quoted as a clean-tree result."""

    okg_commit: str
    okg_tree_status: str
    #: The dataset/corpus manifest digest the measured run consumed.
    corpus_manifest_digest: str
    resource_config_version: int
    resource_config_digest: str
    #: Machine/runtime configuration for performance results (§12).
    machine_runtime_label: str
    #: The frozen-contract version this trace converts into.
    contract_schema_version: str = CONTRACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        where = "$.binding"
        _require_str(self.okg_commit, f"{where}.okg_commit")
        _require_choice(self.okg_tree_status, f"{where}.okg_tree_status",
                        TREE_STATUSES, REFUSAL_NON_CANONICAL_VALUE)
        _require_str(self.corpus_manifest_digest,
                     f"{where}.corpus_manifest_digest")
        _require_str(self.resource_config_digest,
                     f"{where}.resource_config_digest")
        _require_str(self.machine_runtime_label,
                     f"{where}.machine_runtime_label")
        _require_str(self.contract_schema_version,
                     f"{where}.contract_schema_version")
        if (isinstance(self.resource_config_version, bool)
                or not isinstance(self.resource_config_version, int)
                or self.resource_config_version < 0):
            _refuse(REFUSAL_NON_CANONICAL_VALUE,
                    f"{where}.resource_config_version",
                    "expected an integer >= 0")

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "contract_schema_version": self.contract_schema_version,
            "corpus_manifest_digest": self.corpus_manifest_digest,
            "machine_runtime_label": self.machine_runtime_label,
            "okg_commit": self.okg_commit,
            "okg_tree_status": self.okg_tree_status,
            "resource_config_digest": self.resource_config_digest,
            "resource_config_version": self.resource_config_version,
        }


def default_machine_runtime_label() -> str:
    """A stable machine/runtime label (§12). No wall clock, no randomness."""
    return "-".join((
        platform.system().lower() or "unknown",
        platform.machine().lower() or "unknown",
        f"cpython{sys.version_info.major}.{sys.version_info.minor}",
    ))


def resource_declaration_digest(
    declared_resources: Sequence[str],
    *,
    cost_unit: str = COST_UNIT_EXECUTOR_STEPS,
    duration_unit: str = DURATION_UNIT_NANOSECONDS,
    capacities: Mapping[str, MaybeInt] | None = None,
) -> str:
    """Digest the RESOURCE CONFIGURATION a measured run actually used (§12).

    §12 binds the resource configuration, and a binding that does not
    move when the configuration moves states nothing: it is present and
    empty in meaning. This digests the declaration itself — the resource
    names the run's actions answer for, the units they are charged in,
    and any capacities the caller can state — so declaring a different
    resource set, or a different capacity for one, is a different
    binding and therefore a different trace identity.

    ``capacities`` is optional because the parity path enforces none; an
    unstated capacity is absent from the digested object rather than
    written as a zero nobody measured."""
    where = "$.resource_declaration"
    if not declared_resources:
        _refuse(REFUSAL_MISSING_FIELD, f"{where}.declared_resources",
                "an empty resource declaration digests to a constant and "
                "would bind the §12 resource configuration to nothing")
    seen: set[str] = set()
    for resource in declared_resources:
        _require_str(resource, f"{where}.declared_resources")
        if resource in seen:
            _refuse(REFUSAL_DUPLICATE_IDENTITY, f"{where}.declared_resources",
                    f"resource {resource!r} is declared twice")
        seen.add(resource)
    payload: dict[str, Any] = {
        "cost_unit": _require_str(cost_unit, f"{where}.cost_unit"),
        "declared_resources": sorted(declared_resources),
        "duration_unit": _require_str(duration_unit, f"{where}.duration_unit"),
    }
    if capacities is not None:
        if not isinstance(capacities, Mapping):
            _refuse(REFUSAL_MISSING_FIELD, f"{where}.capacities",
                    "expected an object of resource -> capacity")
        for resource, capacity in capacities.items():
            _require_str(resource, f"{where}.capacities")
            if resource not in seen:
                _refuse(REFUSAL_UNDECLARED_RESOURCE,
                        f"{where}.capacities.{resource}",
                        f"resource {resource!r} is not declared by the run")
            _require_maybe_int(capacity, f"{where}.capacities.{resource}")
        payload["capacities"] = {
            r: _quantity(v) for r, v in sorted(capacities.items())
        }
    return object_digest(payload)


def okg_repo_binding(
    *,
    repo_root: Any,
    corpus_manifest_digest: str,
    resource_config_version: int,
    resource_config_digest: str,
    machine_runtime_label: str | None = None,
    run_git: Callable[[Sequence[str]], str] | None = None,
) -> ReproducibilityBinding:
    """Read the live OKG commit and dirty status into a §12 binding.

    ``run_git`` is injectable so a test can bind a known repo state
    without a subprocess. The default shells two read-only commands and
    REFUSES on failure: a binding that guessed at the commit would be
    worse than no binding, because it would look authoritative."""
    root = str(repo_root)

    def _default(argv: Sequence[str]) -> str:
        proc = subprocess.run(
            ["git", "-C", root, *argv],
            capture_output=True, text=True, check=False,
        )
        if proc.returncode != 0:
            _refuse(
                REFUSAL_ADAPTER_SURFACE, "$.binding.okg_commit",
                f"`git {' '.join(argv)}` failed in {root!r} "
                f"(exit {proc.returncode}); the commit binding cannot be "
                "guessed",
                stderr=proc.stderr.strip()[:400],
            )
        return proc.stdout

    runner = run_git or _default
    commit = runner(["rev-parse", "HEAD"]).strip()
    if not commit:
        _refuse(REFUSAL_ADAPTER_SURFACE, "$.binding.okg_commit",
                "git reported no HEAD commit")
    status = runner(["status", "--porcelain"])
    return ReproducibilityBinding(
        okg_commit=commit,
        okg_tree_status=TREE_DIRTY if status.strip() else TREE_CLEAN,
        corpus_manifest_digest=corpus_manifest_digest,
        resource_config_version=resource_config_version,
        resource_config_digest=resource_config_digest,
        machine_runtime_label=(
            machine_runtime_label or default_machine_runtime_label()
        ),
    )


# --------------------------------------------------------------------------- #
# The three §7.1 quantities, as three distinct types.                          #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PlanEstimate:
    """The PINNED estimate planning used, with estimator identity (§7.1).

    This is the only quantity the objective is optimal with respect to.
    It carries no measurement source, because an estimate is not an
    observation; the loader refuses ``measurement_source`` by name."""

    duration: MaybeInt
    cost: MaybeInt
    estimator: EstimatorRef

    def __post_init__(self) -> None:
        where = "$.estimate"
        _require_maybe_int(self.duration, f"{where}.duration")
        _require_maybe_int(self.cost, f"{where}.cost")
        if self.duration == 0:
            _refuse(
                REFUSAL_NON_CANONICAL_VALUE, f"{where}.duration",
                "an estimated duration of zero would exempt the action "
                "from every capacity, conflict, and reservation check "
                "while still earning coverage credit; an unestimated "
                "duration is UNKNOWN",
            )
        if not isinstance(self.estimator, EstimatorRef):
            _refuse(REFUSAL_MISSING_FIELD, f"{where}.estimator",
                    "an estimate must name its estimator and version")
        if self.estimator == ESTIMATOR_UNESTIMATED and not (
            isinstance(self.duration, Unknown)
            and isinstance(self.cost, Unknown)
        ):
            _refuse(
                REFUSAL_ESTIMATOR_DISAGREEMENT, where,
                f"estimator {ESTIMATOR_UNESTIMATED.id!r} declares that NO "
                "estimator exists yet, so every estimated quantity under "
                "it must be UNKNOWN; a stated number under it has no "
                "reviewed provenance",
            )

    @classmethod
    def unestimated(cls, reason: str) -> "PlanEstimate":
        """The honest estimate for a path with no estimator yet (§13)."""
        unknown = Unknown(_require_str(reason, "$.estimate.unknown"))
        return cls(duration=unknown, cost=unknown,
                   estimator=ESTIMATOR_UNESTIMATED)

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "cost": _quantity(self.cost),
            "duration": _quantity(self.duration),
            "estimator": self.estimator.to_canonical_dict(),
        }


@dataclass(frozen=True)
class Reservation:
    """An ENFORCEABLE cap or budget used for admission (§7, §7.1).

    §7 admits a bound only where something can actually enforce it, so
    :attr:`enforced_by` must name the enforcing mechanism. A path with no
    admission control states :data:`UNKNOWN` for the whole reservation
    instead of writing caps nothing checks — an unenforceable bound
    cannot support a hard-budget guarantee."""

    duration_cap: MaybeInt
    cost_cap: MaybeInt
    demand_caps: Mapping[str, MaybeInt]
    enforced_by: str

    def __post_init__(self) -> None:
        where = "$.reservation"
        _require_maybe_int(self.duration_cap, f"{where}.duration_cap")
        _require_maybe_int(self.cost_cap, f"{where}.cost_cap")
        _require_str(self.enforced_by, f"{where}.enforced_by")
        if not isinstance(self.demand_caps, Mapping):
            _refuse(REFUSAL_MISSING_FIELD, f"{where}.demand_caps",
                    "expected an object of resource -> cap")
        for resource, cap in self.demand_caps.items():
            _require_str(resource, f"{where}.demand_caps")
            _require_maybe_int(cap, f"{where}.demand_caps.{resource}")

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "cost_cap": _quantity(self.cost_cap),
            "demand_caps": {
                r: _quantity(v) for r, v in sorted(self.demand_caps.items())
            },
            "duration_cap": _quantity(self.duration_cap),
            "enforced_by": self.enforced_by,
        }


@dataclass(frozen=True)
class RealizedMeasurement:
    """What actually happened, with the instrument NAMED (§7.1).

    Realized values are for reporting and recalibration. They never
    become estimates implicitly: the conversion to the frozen contract
    reads them only under an explicit
    :data:`FIELD_SOURCE_REALIZED_RECALIBRATION` request that must also
    name the recalibration estimator.

    A realized duration of ``0`` REFUSES. Zero does not mean the action
    was free; it means the clock could not resolve the interval, which is
    UNKNOWN with a reason."""

    duration: MaybeInt
    cost: MaybeInt
    measurement_source: str

    def __post_init__(self) -> None:
        where = "$.realized"
        _require_maybe_int(self.duration, f"{where}.duration")
        _require_maybe_int(self.cost, f"{where}.cost")
        _require_str(self.measurement_source, f"{where}.measurement_source")
        if self.duration == 0:
            _refuse(
                REFUSAL_UNMEASURED_ZERO, f"{where}.duration",
                "a realized duration of zero is not a free action, it is "
                "an interval the clock could not resolve; state UNKNOWN "
                "with a reason, or measure in a finer unit",
            )

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "cost": _quantity(self.cost),
            "duration": _quantity(self.duration),
            "measurement_source": self.measurement_source,
        }


# --------------------------------------------------------------------------- #
# One trace, and a set of them.                                                #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ActionTrace:
    """One executed action's measurement record.

    ``provenance`` is a string-to-string map of execution facts that are
    not quantities — the mode, the terminal, the generation id, the
    fallback cause when there was one. It is digested, so a trace cannot
    be re-attributed to a different cycle after the fact."""

    action_id: str
    action_class: str
    cost_unit: str
    duration_unit: str
    estimate: PlanEstimate
    reservation: Reservation | Unknown
    realized: RealizedMeasurement
    demand: Mapping[str, MaybeInt]
    outcome: str
    binding: ReproducibilityBinding
    provenance: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        where = f"$.traces[{self.action_id}]"
        _require_str(self.action_id, f"{where}.action_id")
        _require_choice(self.action_class, f"{where}.action_class",
                        GRANULARITIES, REFUSAL_UNKNOWN_GRANULARITY)
        _require_str(self.cost_unit, f"{where}.cost_unit")
        _require_str(self.duration_unit, f"{where}.duration_unit")
        _require_choice(self.outcome, f"{where}.outcome", OUTCOMES,
                        REFUSAL_UNSUPPORTED_OUTCOME)
        if not isinstance(self.estimate, PlanEstimate):
            _refuse(REFUSAL_MISSING_FIELD, f"{where}.estimate",
                    "expected a PlanEstimate")
        if not isinstance(self.realized, RealizedMeasurement):
            _refuse(REFUSAL_MISSING_FIELD, f"{where}.realized",
                    "expected a RealizedMeasurement")
        if not isinstance(self.reservation, (Reservation, Unknown)):
            _refuse(REFUSAL_MISSING_FIELD, f"{where}.reservation",
                    "expected a Reservation or the UNKNOWN sentinel")
        if not isinstance(self.binding, ReproducibilityBinding):
            _refuse(REFUSAL_MISSING_FIELD, f"{where}.binding",
                    "expected a ReproducibilityBinding (§12)")
        if not isinstance(self.demand, Mapping) or not self.demand:
            _refuse(REFUSAL_MISSING_FIELD, f"{where}.demand",
                    "an action answers for every declared resource; an "
                    "empty demand states nothing")
        for resource, value in self.demand.items():
            _require_str(resource, f"{where}.demand")
            _require_maybe_int(value, f"{where}.demand.{resource}")
        for key, value in self.provenance.items():
            _require_str(key, f"{where}.provenance")
            if not isinstance(value, str):
                _refuse(REFUSAL_NON_CANONICAL_VALUE,
                        f"{where}.provenance.{key}",
                        "provenance values are strings; quantities belong "
                        "in estimate, reservation, realized, or demand")

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "action_class": self.action_class,
            "action_id": self.action_id,
            "binding": self.binding.to_canonical_dict(),
            "cost_unit": self.cost_unit,
            "demand": {
                r: _quantity(v) for r, v in sorted(self.demand.items())
            },
            "duration_unit": self.duration_unit,
            "estimate": self.estimate.to_canonical_dict(),
            "outcome": self.outcome,
            "provenance": dict(sorted(self.provenance.items())),
            "realized": self.realized.to_canonical_dict(),
            "reservation": self.reservation.to_canonical_dict(),
        }

    @property
    def digest(self) -> str:
        return object_digest(self.to_canonical_dict())


@dataclass(frozen=True)
class ActionTraceSet:
    """Traces from ONE measured run at ONE declared granularity.

    Units and declared resources live here so that every membership rule
    is checked once, at the boundary: a trace whose cost unit differs
    from the set's refuses (§7.1 forbids adding unlike units), and a
    trace that fails to answer for a declared resource refuses rather
    than reading as zero (§7)."""

    schema_version: str
    trace_set_id: str
    epoch: int
    granularity: str
    cost_unit: str
    duration_unit: str
    declared_resources: tuple[str, ...]
    traces: tuple[ActionTrace, ...]

    def __post_init__(self) -> None:
        where = "$"
        if self.schema_version != MEASUREMENT_SCHEMA_VERSION:
            _refuse(REFUSAL_SCHEMA_VERSION, f"{where}.schema_version",
                    f"expected {MEASUREMENT_SCHEMA_VERSION!r}, got "
                    f"{self.schema_version!r}")
        _require_str(self.trace_set_id, f"{where}.trace_set_id")
        _require_choice(self.granularity, f"{where}.granularity",
                        GRANULARITIES, REFUSAL_UNKNOWN_GRANULARITY)
        _require_str(self.cost_unit, f"{where}.cost_unit")
        _require_str(self.duration_unit, f"{where}.duration_unit")
        if (isinstance(self.epoch, bool) or not isinstance(self.epoch, int)
                or self.epoch < 0):
            _refuse(REFUSAL_NON_CANONICAL_VALUE, f"{where}.epoch",
                    "expected an integer >= 0")
        if not self.declared_resources:
            _refuse(REFUSAL_MISSING_FIELD, f"{where}.declared_resources",
                    "a measured run declares the resources its actions "
                    "answer for; an empty declaration makes every demand "
                    "vacuous")
        seen: set[str] = set()
        for resource in self.declared_resources:
            _require_str(resource, f"{where}.declared_resources")
            if resource in seen:
                _refuse(REFUSAL_DUPLICATE_IDENTITY,
                        f"{where}.declared_resources",
                        f"resource {resource!r} is declared twice")
            seen.add(resource)

        ids: set[str] = set()
        for trace in self.traces:
            if not isinstance(trace, ActionTrace):
                _refuse(REFUSAL_MISSING_FIELD, f"{where}.traces",
                        "expected ActionTrace values")
            spot = f"{where}.traces[{trace.action_id}]"
            if trace.action_id in ids:
                _refuse(REFUSAL_DUPLICATE_IDENTITY, spot,
                        f"action id {trace.action_id!r} is traced twice")
            ids.add(trace.action_id)
            if trace.cost_unit != self.cost_unit:
                _refuse(
                    REFUSAL_MIXED_COST_UNITS, f"{spot}.cost_unit",
                    f"trace declares {trace.cost_unit!r} but the set "
                    f"declares {self.cost_unit!r}; unlike units may not be "
                    "added (design §7.1)",
                )
            if trace.duration_unit != self.duration_unit:
                _refuse(
                    REFUSAL_MIXED_DURATION_UNITS, f"{spot}.duration_unit",
                    f"trace declares {trace.duration_unit!r} but the set "
                    f"declares {self.duration_unit!r}; unlike units may "
                    "not be added",
                )
            if trace.action_class != self.granularity:
                _refuse(
                    REFUSAL_UNKNOWN_GRANULARITY, f"{spot}.action_class",
                    f"trace is a {trace.action_class!r} action in a "
                    f"{self.granularity!r} set; the same work appears at "
                    "both granularities and summing a mixed set would "
                    "double-charge it",
                )
            for resource in self.declared_resources:
                if resource not in trace.demand:
                    _refuse(
                        REFUSAL_MISSING_RESOURCE_DEMAND,
                        f"{spot}.demand.{resource}",
                        f"resource {resource!r} is declared but this "
                        "action does not answer for it; an omitted demand "
                        "is not zero (design §7)",
                    )
            for resource in trace.demand:
                if resource not in seen:
                    _refuse(
                        REFUSAL_UNDECLARED_RESOURCE,
                        f"{spot}.demand.{resource}",
                        f"resource {resource!r} is not declared by the set",
                    )
            if isinstance(trace.reservation, Reservation):
                for resource in trace.reservation.demand_caps:
                    if resource not in seen:
                        _refuse(
                            REFUSAL_UNDECLARED_RESOURCE,
                            f"{spot}.reservation.demand_caps.{resource}",
                            f"resource {resource!r} is not declared by the "
                            "set, so no cap on it could be enforced",
                        )

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "cost_unit": self.cost_unit,
            "declared_resources": sorted(self.declared_resources),
            "duration_unit": self.duration_unit,
            "epoch": self.epoch,
            "granularity": self.granularity,
            "schema_version": self.schema_version,
            "trace_set_id": self.trace_set_id,
            "traces": [
                t.to_canonical_dict()
                for t in sorted(self.traces, key=lambda t: t.action_id)
            ],
        }

    @property
    def digest(self) -> str:
        return object_digest(self.to_canonical_dict())

    def canonical_json(self) -> str:
        return canonical_json(self.to_canonical_dict())

    def trace_by_id(self, action_id: str) -> ActionTrace | None:
        for trace in self.traces:
            if trace.action_id == action_id:
                return trace
        return None


def trace_set_digest(trace_set: ActionTraceSet) -> str:
    return trace_set.digest


# --------------------------------------------------------------------------- #
# The loader: fail-closed, and the realized/estimate confusion is NAMED.       #
# --------------------------------------------------------------------------- #
#: Keys that belong only to a realized measurement. Seeing one inside an
#: estimate is not a generic typo; it is the specific §7.1 dishonesty of
#: presenting an observation as a plan input, so it gets its own code.
_REALIZED_ONLY_KEYS = ("measurement_source",)
#: The mirror image: an estimator inside a realized measurement.
_ESTIMATE_ONLY_KEYS = ("estimator",)


def _fields(raw: Mapping[str, Any], where: str, required: Sequence[str],
            optional: Sequence[str] = ()) -> None:
    allowed = set(required) | set(optional)
    for key in sorted(raw):
        if key not in allowed:
            _refuse(REFUSAL_UNKNOWN_FIELD, f"{where}.{key}",
                    "field is not part of the measurement contract")
    for key in required:
        if key not in raw:
            _refuse(REFUSAL_MISSING_FIELD, f"{where}.{key}",
                    "required field is absent; nothing defaults silently")


def _require_mapping(value: Any, where: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        _refuse(REFUSAL_MISSING_FIELD, where, "expected an object")
    return value


def _load_maybe_int(value: Any, where: str) -> MaybeInt:
    if isinstance(value, Mapping):
        _fields(value, where, ["unknown"])
        return Unknown(_require_str(value["unknown"], f"{where}.unknown"))
    return _require_maybe_int(value, where)


def _load_estimator(raw: Any, where: str) -> EstimatorRef:
    raw = _require_mapping(raw, where)
    _fields(raw, where, ["id", "version"])
    version = raw["version"]
    if isinstance(version, bool) or not isinstance(version, int) or version < 0:
        _refuse(REFUSAL_NON_CANONICAL_VALUE, f"{where}.version",
                "expected an integer >= 0")
    return EstimatorRef(id=_require_str(raw["id"], f"{where}.id"),
                        version=version)


def _load_estimate(raw: Any, where: str) -> PlanEstimate:
    raw = _require_mapping(raw, where)
    for key in _REALIZED_ONLY_KEYS:
        if key in raw:
            _refuse(
                REFUSAL_REALIZED_AS_ESTIMATE, f"{where}.{key}",
                f"{key!r} belongs to a realized measurement; an estimate "
                "that carries one is presenting an observation as a plan "
                "input, which is exactly what design §7.1 separates",
            )
    _fields(raw, where, ["cost", "duration", "estimator"])
    return PlanEstimate(
        duration=_load_maybe_int(raw["duration"], f"{where}.duration"),
        cost=_load_maybe_int(raw["cost"], f"{where}.cost"),
        estimator=_load_estimator(raw["estimator"], f"{where}.estimator"),
    )


def _load_realized(raw: Any, where: str) -> RealizedMeasurement:
    raw = _require_mapping(raw, where)
    for key in _ESTIMATE_ONLY_KEYS:
        if key in raw:
            _refuse(
                REFUSAL_REALIZED_AS_ESTIMATE, f"{where}.{key}",
                f"{key!r} belongs to an estimate; a measurement is "
                "produced by an instrument, not by an estimator",
            )
    _fields(raw, where, ["cost", "duration", "measurement_source"])
    return RealizedMeasurement(
        duration=_load_maybe_int(raw["duration"], f"{where}.duration"),
        cost=_load_maybe_int(raw["cost"], f"{where}.cost"),
        measurement_source=_require_str(raw["measurement_source"],
                                        f"{where}.measurement_source"),
    )


def _load_reservation(raw: Any, where: str) -> Reservation | Unknown:
    raw = _require_mapping(raw, where)
    if "unknown" in raw:
        _fields(raw, where, ["unknown"])
        return Unknown(_require_str(raw["unknown"], f"{where}.unknown"))
    _fields(raw, where,
            ["cost_cap", "demand_caps", "duration_cap", "enforced_by"])
    caps_raw = _require_mapping(raw["demand_caps"], f"{where}.demand_caps")
    return Reservation(
        duration_cap=_load_maybe_int(raw["duration_cap"],
                                     f"{where}.duration_cap"),
        cost_cap=_load_maybe_int(raw["cost_cap"], f"{where}.cost_cap"),
        demand_caps={
            r: _load_maybe_int(v, f"{where}.demand_caps.{r}")
            for r, v in sorted(caps_raw.items())
        },
        enforced_by=_require_str(raw["enforced_by"], f"{where}.enforced_by"),
    )


def _load_binding(raw: Any, where: str) -> ReproducibilityBinding:
    raw = _require_mapping(raw, where)
    _fields(raw, where, [
        "contract_schema_version", "corpus_manifest_digest",
        "machine_runtime_label", "okg_commit", "okg_tree_status",
        "resource_config_digest", "resource_config_version",
    ])
    version = raw["resource_config_version"]
    if isinstance(version, bool) or not isinstance(version, int):
        _refuse(REFUSAL_NON_CANONICAL_VALUE,
                f"{where}.resource_config_version", "expected an integer")
    return ReproducibilityBinding(
        okg_commit=_require_str(raw["okg_commit"], f"{where}.okg_commit"),
        okg_tree_status=_require_choice(
            raw["okg_tree_status"], f"{where}.okg_tree_status",
            TREE_STATUSES, REFUSAL_NON_CANONICAL_VALUE),
        corpus_manifest_digest=_require_str(
            raw["corpus_manifest_digest"], f"{where}.corpus_manifest_digest"),
        resource_config_version=version,
        resource_config_digest=_require_str(
            raw["resource_config_digest"], f"{where}.resource_config_digest"),
        machine_runtime_label=_require_str(
            raw["machine_runtime_label"], f"{where}.machine_runtime_label"),
        contract_schema_version=_require_str(
            raw["contract_schema_version"],
            f"{where}.contract_schema_version"),
    )


def _load_trace(raw: Any, where: str) -> ActionTrace:
    raw = _require_mapping(raw, where)
    _fields(raw, where, [
        "action_class", "action_id", "binding", "cost_unit", "demand",
        "duration_unit", "estimate", "outcome", "provenance", "realized",
        "reservation",
    ])
    demand_raw = _require_mapping(raw["demand"], f"{where}.demand")
    provenance_raw = _require_mapping(raw["provenance"], f"{where}.provenance")
    return ActionTrace(
        action_id=_require_str(raw["action_id"], f"{where}.action_id"),
        action_class=_require_str(raw["action_class"],
                                  f"{where}.action_class"),
        cost_unit=_require_str(raw["cost_unit"], f"{where}.cost_unit"),
        duration_unit=_require_str(raw["duration_unit"],
                                   f"{where}.duration_unit"),
        estimate=_load_estimate(raw["estimate"], f"{where}.estimate"),
        reservation=_load_reservation(raw["reservation"],
                                      f"{where}.reservation"),
        realized=_load_realized(raw["realized"], f"{where}.realized"),
        demand={
            r: _load_maybe_int(v, f"{where}.demand.{r}")
            for r, v in sorted(demand_raw.items())
        },
        outcome=_require_str(raw["outcome"], f"{where}.outcome"),
        binding=_load_binding(raw["binding"], f"{where}.binding"),
        provenance={
            k: _require_str(v, f"{where}.provenance.{k}")
            for k, v in sorted(provenance_raw.items())
        },
    )


def load_trace_set(raw: Any) -> ActionTraceSet:
    """Parse and validate one trace set, fail-closed.

    Refuses on an unknown field, a missing field, a duplicate action id,
    a mixed cost or duration unit, a mixed granularity, an undeclared or
    unanswered resource, an estimate carrying a measurement source, and a
    measurement carrying an estimator."""
    where = "$"
    raw = _require_mapping(raw, where)
    _fields(raw, where, [
        "cost_unit", "declared_resources", "duration_unit", "epoch",
        "granularity", "schema_version", "trace_set_id", "traces",
    ])
    resources_raw = raw["declared_resources"]
    if not isinstance(resources_raw, (list, tuple)):
        _refuse(REFUSAL_NON_CANONICAL_VALUE, f"{where}.declared_resources",
                "expected a list of resource names")
    traces_raw = raw["traces"]
    if not isinstance(traces_raw, (list, tuple)):
        _refuse(REFUSAL_NON_CANONICAL_VALUE, f"{where}.traces",
                "expected a list of traces")
    epoch = raw["epoch"]
    if isinstance(epoch, bool) or not isinstance(epoch, int):
        _refuse(REFUSAL_NON_CANONICAL_VALUE, f"{where}.epoch",
                "expected an integer")
    return ActionTraceSet(
        schema_version=_require_str(raw["schema_version"],
                                    f"{where}.schema_version"),
        trace_set_id=_require_str(raw["trace_set_id"],
                                  f"{where}.trace_set_id"),
        epoch=epoch,
        granularity=_require_str(raw["granularity"], f"{where}.granularity"),
        cost_unit=_require_str(raw["cost_unit"], f"{where}.cost_unit"),
        duration_unit=_require_str(raw["duration_unit"],
                                   f"{where}.duration_unit"),
        declared_resources=tuple(
            _require_str(r, f"{where}.declared_resources[{i}]")
            for i, r in enumerate(resources_raw)
        ),
        traces=tuple(
            _load_trace(item, f"{where}.traces[{i}]")
            for i, item in enumerate(traces_raw)
        ),
    )


# --------------------------------------------------------------------------- #
# Conversion into the frozen contract's action fields.                         #
# --------------------------------------------------------------------------- #
#: The exact keys one converted fragment carries. Spelled out so a
#: hand-built :class:`ContractActionFields` is held to the same shape the
#: conversion produces, rather than certifying whatever was handed in.
_CONTRACT_FRAGMENT_FIELDS = (
    "cost_unit", "demand", "duration", "estimator", "execution_cost",
)


@dataclass(frozen=True)
class ContractActionFields:
    """The action-cost/duration fragments a batch instance can absorb.

    ``fields`` maps action id to the ``duration`` / ``execution_cost`` /
    ``cost_unit`` / ``demand`` / ``estimator`` sub-object of a
    :class:`...batch_schedule.Action`. It is deliberately a FRAGMENT: an
    instance also needs coverage, prerequisites, conflicts, effects, and
    revelations, none of which a measurement can supply, and inventing
    them here would let a measured run masquerade as a planned batch."""

    cost_unit: str
    duration_unit: str
    field_source: str
    fields: Mapping[str, Mapping[str, Any]]

    def __post_init__(self) -> None:
        where = "$.contract_action_fields"
        _require_str(self.cost_unit, f"{where}.cost_unit")
        _require_str(self.duration_unit, f"{where}.duration_unit")
        _require_choice(self.field_source, f"{where}.field_source",
                        FIELD_SOURCES, REFUSAL_UNKNOWN_FIELD_SOURCE)
        if not isinstance(self.fields, Mapping) or not self.fields:
            _refuse(
                REFUSAL_MISSING_FIELD, f"{where}.fields",
                "expected a non-empty object of action id -> fragment; an "
                "empty conversion states nothing and would still certify a "
                "digest",
            )
        for action_id, fragment in self.fields.items():
            _require_str(action_id, f"{where}.fields")
            spot = f"{where}.fields[{action_id}]"
            if not isinstance(fragment, Mapping):
                _refuse(REFUSAL_MISSING_FIELD, spot,
                        "expected a contract action fragment object")
            _fields(fragment, spot, _CONTRACT_FRAGMENT_FIELDS)
            if fragment["cost_unit"] != self.cost_unit:
                _refuse(
                    REFUSAL_MIXED_COST_UNITS, f"{spot}.cost_unit",
                    f"fragment declares {fragment['cost_unit']!r} but the "
                    f"conversion declares {self.cost_unit!r}; unlike units "
                    "may not be added (design §7.1)",
                )
            if fragment["duration"] == 0:
                _refuse(
                    REFUSAL_NON_CANONICAL_VALUE, f"{spot}.duration",
                    "the frozen contract refuses a zero-length action",
                )

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "cost_unit": self.cost_unit,
            "duration_unit": self.duration_unit,
            "field_source": self.field_source,
            "fields": {k: dict(v) for k, v in sorted(self.fields.items())},
        }

    @property
    def digest(self) -> str:
        return object_digest(self.to_canonical_dict())


def _fragment(trace: ActionTrace, *, field_source: str,
              recalibration_estimator: EstimatorRef | None,
              include_setup_charged_realized: bool = False) -> dict[str, Any]:
    if field_source == FIELD_SOURCE_ESTIMATE:
        duration: MaybeInt = trace.estimate.duration
        cost: MaybeInt = trace.estimate.cost
        estimator = trace.estimate.estimator
    else:
        if recalibration_estimator is None:
            _refuse(
                REFUSAL_REALIZED_AS_ESTIMATE, f"$.traces[{trace.action_id}]",
                "reading realized measurements as contract estimates is a "
                "recalibration, and §7.1 requires the estimator that "
                "performs it to be named and versioned; pass "
                "recalibration_estimator",
            )
        if recalibration_estimator == ESTIMATOR_UNESTIMATED:
            _refuse(
                REFUSAL_ESTIMATOR_DISAGREEMENT,
                f"$.traces[{trace.action_id}]",
                f"{ESTIMATOR_UNESTIMATED.id!r} declares that no estimator "
                "exists; it cannot also be the estimator that recalibrated "
                "from measurements",
            )
        source = trace.realized.measurement_source
        if (source in SETUP_CHARGED_MEASUREMENT_SOURCES
                and not include_setup_charged_realized):
            _refuse(
                REFUSAL_SETUP_CHARGED_REALIZED,
                f"$.traces[{trace.action_id}].realized",
                f"instrument {source!r} declares that this interval also "
                "covers one-time driver setup that is not this action's "
                "own work; recalibrating an estimate from it would charge "
                "every later run for that setup, so it is excluded unless "
                "the caller asks for it by name "
                "(include_setup_charged_realized=True)",
                measurement_source=source,
            )
        duration = trace.realized.duration
        cost = trace.realized.cost
        estimator = recalibration_estimator
    if duration == 0:
        _refuse(
            REFUSAL_NON_CANONICAL_VALUE,
            f"$.traces[{trace.action_id}].duration",
            "the frozen contract refuses a zero-length action; a "
            "zero-length interval satisfies no `start <= t < end`",
        )
    return {
        "duration": _quantity(duration),
        "execution_cost": _quantity(cost),
        "cost_unit": trace.cost_unit,
        "demand": {r: _quantity(v) for r, v in sorted(trace.demand.items())},
        "estimator": estimator.to_canonical_dict(),
    }


def contract_action_fields(
    trace_sets: ActionTraceSet | Iterable[ActionTraceSet],
    *,
    field_source: str = FIELD_SOURCE_ESTIMATE,
    recalibration_estimator: EstimatorRef | None = None,
    include_setup_charged_realized: bool = False,
) -> ContractActionFields:
    """Convert traces into the frozen contract's action cost/duration fields.

    Integers pass through in the declared unit and :data:`UNKNOWN` passes
    through as UNKNOWN — never as zero. Sets with unlike cost or duration
    units REFUSE rather than being added (§7.1).

    Sets at UNLIKE GRANULARITIES also REFUSE. The same execution appears
    once as a publish cycle and again as its executor steps, so merging
    the two accounts the parity adapter returns charges the same work
    twice: the checker consumes a fragment's ``demand`` multiplied by its
    duration, so a merged set inflates every resource account by exactly
    the duplicated work. One conversion answers at one granularity.

    ``field_source`` defaults to :data:`FIELD_SOURCE_ESTIMATE`, so the
    contract's estimate fields are filled from the trace's ESTIMATE. On
    a path with no estimator that yields UNKNOWN, which is the honest
    answer; realized measurements do not leak in by default, and asking
    for them requires naming the recalibration estimator that stands
    behind the substitution.

    ``include_setup_charged_realized`` governs the one realized
    instrument that measures more than its own action — see
    :data:`SETUP_CHARGED_MEASUREMENT_SOURCES`. It applies only under
    :data:`FIELD_SOURCE_REALIZED_RECALIBRATION`, because that is the only
    source that reads a realized number at all."""
    if isinstance(trace_sets, ActionTraceSet):
        sets: tuple[ActionTraceSet, ...] = (trace_sets,)
    else:
        sets = tuple(trace_sets)
    if not sets:
        _refuse(REFUSAL_MISSING_FIELD, "$",
                "no trace set was supplied; there is nothing to convert")
    field_source = _require_choice(field_source, "$.field_source",
                                   FIELD_SOURCES,
                                   REFUSAL_UNKNOWN_FIELD_SOURCE)
    cost_unit = sets[0].cost_unit
    duration_unit = sets[0].duration_unit
    granularity = sets[0].granularity
    fields: dict[str, Mapping[str, Any]] = {}
    for index, trace_set in enumerate(sets):
        where = f"$.trace_sets[{index}]"
        if trace_set.granularity != granularity:
            _refuse(
                REFUSAL_UNKNOWN_GRANULARITY, f"{where}.granularity",
                f"set declares {trace_set.granularity!r} but the conversion "
                f"already declared {granularity!r}; the same work appears at "
                "both granularities, so a merged fragment set charges every "
                "resource twice for one execution (design §7, §7.1)",
            )
        if trace_set.cost_unit != cost_unit:
            _refuse(
                REFUSAL_MIXED_COST_UNITS, f"{where}.cost_unit",
                f"set declares {trace_set.cost_unit!r} but the conversion "
                f"already declared {cost_unit!r}; unlike units may not be "
                "summed without a pinned conversion rule (design §7.1)",
            )
        if trace_set.duration_unit != duration_unit:
            _refuse(
                REFUSAL_MIXED_DURATION_UNITS, f"{where}.duration_unit",
                f"set declares {trace_set.duration_unit!r} but the "
                f"conversion already declared {duration_unit!r}",
            )
        for trace in trace_set.traces:
            if trace.action_id in fields:
                _refuse(REFUSAL_DUPLICATE_IDENTITY,
                        f"{where}.traces[{trace.action_id}]",
                        f"action id {trace.action_id!r} appears in more "
                        "than one supplied set")
            fields[trace.action_id] = _fragment(
                trace, field_source=field_source,
                recalibration_estimator=recalibration_estimator,
                include_setup_charged_realized=include_setup_charged_realized,
            )
    return ContractActionFields(
        cost_unit=cost_unit, duration_unit=duration_unit,
        field_source=field_source, fields=fields,
    )


# --------------------------------------------------------------------------- #
# The adapter over the EXISTING parity execution path.                         #
# --------------------------------------------------------------------------- #
# Structural duck typing, on purpose. This module imports nothing from
# `okg.bench`: it reads the attributes `parity.run_update` ALREADY returns
# on its `UpdateExecution` / `LedgerRow`, and refuses loudly when one is
# absent rather than defaulting. So there is no import edge from the
# substrate into the benchmark, no patch to the execution path, and
# nothing on the fixed-point, certificate, fallback, or repair-selection
# paths runs differently — or at all — because of these functions.
# Duck typing is NOT the same as trusting whatever turns up. A surface
# check that only asks `hasattr` catches an attribute that was RENAMED and
# misses one that was RETYPED, and a retyped attribute is the likelier
# drift: a count that becomes a `float`, a `Decimal`, or a `numpy` integer
# passes presence, passes `int()`, and lands in a digested trace as a
# truncated number that looks exact. `int()` on an unvalidated attribute is
# exactly the unannounced coercion this module exists to refuse, and a
# `terminal` whose `__eq__` merely answers True would forge the one value
# that discharges an obligation. So every consumed attribute is read
# through a typed extractor that names the type it required.
_EXECUTION_ATTRS = ("mode", "index", "ledger", "run_steps", "skip_outcomes")
_LEDGER_ATTRS = (
    "steps_run", "steps_reached", "certificates_written", "terminal",
    "fallback_cause", "published", "generation_id",
)

#: Why the parity path has no estimate to pin.
NO_ESTIMATOR_REASON = (
    "no duration or cost estimator exists for the reuse-pass execution "
    "path; design §13 defers the estimator to a later slice, and an "
    "unreviewed estimate would make the objective optimal with respect "
    "to a number nobody approved"
)
#: Why the parity path has no enforceable reservation.
NO_RESERVATION_REASON = (
    "the parity replay path admits every reached step and enforces no "
    "duration, cost, or resource cap, so no bound here could support a "
    "hard-budget guarantee (design §7)"
)
#: Why a step's realized duration is not measurable today.
NO_STEP_CLOCK_REASON = (
    "per-step wall clock is not instrumented; only the enclosing publish "
    "cycle is timed, and dividing the cycle interval across its steps "
    "would be a fabricated measurement"
)
#: Why per-step certificate work is not attributable.
NO_STEP_CERTIFICATE_REASON = (
    "the parity ledger charges certificate writes to the cycle, not to "
    "an individual step"
)
#: What a skip costs. Recorded on the cycle trace so the answer is
#: explicit rather than an absence a reader has to interpret.
SKIP_COST_STATEMENT = (
    "a skipped step performs no execution; its certificate-validation "
    "cost is not exposed per action by the parity ledger, so it is "
    "UNKNOWN rather than zero"
)


def _require_attrs(obj: Any, attrs: Sequence[str], where: str) -> None:
    for attr in attrs:
        if not hasattr(obj, attr):
            _refuse(
                REFUSAL_ADAPTER_SURFACE, f"{where}.{attr}",
                f"the measured object does not expose {attr!r}; the "
                "adapter reads the execution path's own record and "
                "refuses to guess at a surface that moved",
            )


def _surface_refuse(where: str, expected: str, value: Any) -> None:
    _refuse(
        REFUSAL_ADAPTER_SURFACE, where,
        f"expected {expected}, got {type(value).__name__}; the adapter "
        "refuses to guess at a surface that moved — including one that "
        "moved in TYPE rather than in name, because coercing it would "
        "record an unannounced conversion as an exact measurement",
        observed_type=type(value).__name__,
    )


def _attr(obj: Any, attr: str, where: str) -> Any:
    _require_attrs(obj, (attr,), where)
    return getattr(obj, attr)


def _surface_str(obj: Any, attr: str, where: str) -> str:
    value = _attr(obj, attr, where)
    if not isinstance(value, str) or not value:
        _surface_refuse(f"{where}.{attr}", "a non-empty str", value)
    return value


def _surface_optional_str(obj: Any, attr: str, where: str) -> str | None:
    value = _attr(obj, attr, where)
    if value is None:
        return None
    if not isinstance(value, str):
        _surface_refuse(f"{where}.{attr}", "a str or None", value)
    return value


def _surface_int(obj: Any, attr: str, where: str, *, minimum: int = 0) -> int:
    value = _attr(obj, attr, where)
    # `bool` is an `int` subclass, and a count that arrived as True would
    # be charged as one unit of work nobody measured.
    if isinstance(value, bool) or not isinstance(value, int):
        _surface_refuse(f"{where}.{attr}", "an int (never a float or a str)",
                        value)
    if value < minimum:
        _refuse(REFUSAL_ADAPTER_SURFACE, f"{where}.{attr}",
                f"expected an integer >= {minimum}, got {value!r}")
    return value


def _surface_bool(obj: Any, attr: str, where: str) -> bool:
    value = _attr(obj, attr, where)
    # Truthiness is not the question. `"false"` is a truthy str, and
    # reading it as a publish commit would charge a commit that never
    # happened.
    if not isinstance(value, bool):
        _surface_refuse(f"{where}.{attr}", "a bool", value)
    return value


def _surface_str_sequence(obj: Any, attr: str, where: str) -> tuple[str, ...]:
    value = _attr(obj, attr, where)
    # A `str` IS a Sequence, and iterating one mints a trace per
    # CHARACTER.
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        _surface_refuse(f"{where}.{attr}",
                        "a sequence of str (a str is not one)", value)
    for position, item in enumerate(value):
        if not isinstance(item, str) or not item:
            _surface_refuse(f"{where}.{attr}[{position}]", "a non-empty str",
                            item)
    return tuple(value)


def _surface_pair_sequence(
    obj: Any, attr: str, where: str
) -> tuple[tuple[str, str], ...]:
    value = _attr(obj, attr, where)
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        _surface_refuse(f"{where}.{attr}",
                        "a sequence of (step_id, cause) pairs", value)
    pairs: list[tuple[str, str]] = []
    for position, item in enumerate(value):
        spot = f"{where}.{attr}[{position}]"
        if (isinstance(item, (str, bytes)) or not isinstance(item, Sequence)
                or len(item) != 2):
            _surface_refuse(spot, "a (step_id, cause) pair", item)
        step_id, cause = item
        if not isinstance(step_id, str) or not step_id:
            _surface_refuse(f"{spot}[0]", "a non-empty step id str", step_id)
        if not isinstance(cause, str):
            _surface_refuse(f"{spot}[1]", "a cause str", cause)
        pairs.append((step_id, cause))
    return tuple(pairs)


@dataclass(frozen=True)
class _ExecutionFacts:
    """The execution surface, read once and TYPED."""

    mode: str
    index: int
    run_steps: tuple[str, ...]
    skip_outcomes: tuple[tuple[str, str], ...]
    steps_run: int
    steps_reached: int
    certificates_written: int
    terminal: str
    fallback_cause: str | None
    published: bool
    generation_id: str

    @property
    def action_id(self) -> str:
        return f"{self.mode}/update-{self.index:04d}"

    @property
    def reached_fixed_point(self) -> bool:
        return self.terminal == TERMINAL_FIXED_POINT


def _read_execution(execution: Any, where: str = "$.execution"
                    ) -> _ExecutionFacts:
    """Read the measured execution's surface, refusing a moved TYPE too."""
    _require_attrs(execution, _EXECUTION_ATTRS, where)
    ledger = execution.ledger
    ledger_where = f"{where}.ledger"
    _require_attrs(ledger, _LEDGER_ATTRS, ledger_where)
    return _ExecutionFacts(
        mode=_surface_str(execution, "mode", where),
        index=_surface_int(execution, "index", where),
        run_steps=_surface_str_sequence(execution, "run_steps", where),
        skip_outcomes=_surface_pair_sequence(execution, "skip_outcomes",
                                             where),
        steps_run=_surface_int(ledger, "steps_run", ledger_where),
        steps_reached=_surface_int(ledger, "steps_reached", ledger_where),
        certificates_written=_surface_int(ledger, "certificates_written",
                                          ledger_where),
        terminal=_surface_str(ledger, "terminal", ledger_where),
        fallback_cause=_surface_optional_str(ledger, "fallback_cause",
                                             ledger_where),
        published=_surface_bool(ledger, "published", ledger_where),
        generation_id=_surface_str(ledger, "generation_id", ledger_where),
    )


def _demand_for(resource: str, known: Mapping[str, MaybeInt],
                unknown_reason: Mapping[str, str],
                default_reason: str) -> MaybeInt:
    if resource in known:
        return known[resource]
    return Unknown(unknown_reason.get(resource, default_reason))


def skipped_step_ids(execution: Any) -> tuple[str, ...]:
    """The step ids the scheduler SKIPPED in one update, sorted.

    A skip produces no realized execution trace: nothing ran, so there is
    nothing to measure. This function exists so a consumer can state that
    positively — the skipped set is nameable — rather than inferring it
    from the absence of traces."""
    return tuple(sorted(
        step for step, _cause in
        _surface_pair_sequence(execution, "skip_outcomes", "$.execution")
    ))


def cycle_trace_from_execution(
    execution: Any,
    *,
    realized_duration: MaybeInt,
    binding: ReproducibilityBinding,
    declared_resources: Sequence[str] = PARITY_RESOURCES,
    measurement_source: str = MEASUREMENT_SOURCE_CYCLE_INTERVAL,
    duration_unit: str = DURATION_UNIT_NANOSECONDS,
    cost_unit: str = COST_UNIT_EXECUTOR_STEPS,
) -> ActionTrace:
    """One publish-cycle trace derived from a completed update execution.

    The realized cost is the ledger's OWN charged work (``steps_run``);
    the realized duration is whatever the caller's clock measured around
    the call. Nothing is recomputed and nothing is re-executed."""
    facts = _read_execution(execution)
    known: dict[str, MaybeInt] = {
        PARITY_RESOURCE_EXECUTOR_STEPS: facts.steps_run,
        PARITY_RESOURCE_CERTIFICATE_WRITES: facts.certificates_written,
        PARITY_RESOURCE_PUBLISH_COMMITS: 1 if facts.published else 0,
    }
    demand = {
        resource: _demand_for(
            resource, known, {},
            f"the parity ledger does not measure resource {resource!r}",
        )
        for resource in declared_resources
    }
    provenance = {
        "certificate_validation_cost": SKIP_COST_STATEMENT,
        "generation_id": facts.generation_id,
        "mode": facts.mode,
        "run_step_occurrences": str(len(facts.run_steps)),
        "steps_reached": str(facts.steps_reached),
        "steps_skipped": str(len(facts.skip_outcomes)),
        "terminal": facts.terminal,
        "update": str(facts.index),
    }
    if facts.fallback_cause:
        provenance["fallback_cause"] = facts.fallback_cause
    return ActionTrace(
        action_id=facts.action_id,
        action_class=GRANULARITY_PUBLISH_CYCLE,
        cost_unit=cost_unit,
        duration_unit=duration_unit,
        estimate=PlanEstimate.unestimated(NO_ESTIMATOR_REASON),
        reservation=Unknown(NO_RESERVATION_REASON),
        realized=RealizedMeasurement(
            duration=realized_duration,
            cost=facts.steps_run,
            measurement_source=_require_str(
                measurement_source, "$.realized.measurement_source"),
        ),
        demand=demand,
        outcome=(OUTCOME_SUCCESS if facts.reached_fixed_point
                 else OUTCOME_UNKNOWN),
        binding=binding,
        provenance=provenance,
    )


def step_traces_from_execution(
    execution: Any,
    *,
    binding: ReproducibilityBinding,
    declared_resources: Sequence[str] = PARITY_RESOURCES,
    duration_unit: str = DURATION_UNIT_NANOSECONDS,
    cost_unit: str = COST_UNIT_EXECUTOR_STEPS,
) -> tuple[ActionTrace, ...]:
    """One trace per EXECUTED step occurrence. Skipped steps yield none.

    The action id carries the occurrence ordinal because a worklist may
    reach the same step more than once in a pass, and each execution is a
    separate action with its own charged work.

    Realized duration is UNKNOWN with a named reason: only the enclosing
    cycle is timed today. Splitting the cycle interval across its steps
    would be an invented measurement, and §7 says an unknown quantity is
    represented as unknown, never as a number."""
    facts = _read_execution(execution)
    reached_fixed_point = facts.reached_fixed_point
    unknown_reasons = {
        PARITY_RESOURCE_CERTIFICATE_WRITES: NO_STEP_CERTIFICATE_REASON,
        PARITY_RESOURCE_PUBLISH_COMMITS: (
            "the publish commit is a cycle-level act, not a step-level one"
        ),
    }
    traces: list[ActionTrace] = []
    for ordinal, step_id in enumerate(facts.run_steps):
        known: dict[str, MaybeInt] = {PARITY_RESOURCE_EXECUTOR_STEPS: 1}
        demand = {
            resource: _demand_for(
                resource, known, unknown_reasons,
                f"the parity ledger does not measure resource {resource!r} "
                "per step",
            )
            for resource in declared_resources
        }
        traces.append(ActionTrace(
            action_id=f"{facts.action_id}/{ordinal:03d}/{step_id}",
            action_class=GRANULARITY_EXECUTOR_STEP,
            cost_unit=cost_unit,
            duration_unit=duration_unit,
            estimate=PlanEstimate.unestimated(NO_ESTIMATOR_REASON),
            reservation=Unknown(NO_RESERVATION_REASON),
            realized=RealizedMeasurement(
                duration=Unknown(NO_STEP_CLOCK_REASON),
                cost=1,
                measurement_source=MEASUREMENT_SOURCE_LEDGER,
            ),
            demand=demand,
            outcome=(OUTCOME_SUCCESS if reached_fixed_point
                     else OUTCOME_UNKNOWN),
            binding=binding,
            provenance={
                "mode": facts.mode,
                "occurrence": str(ordinal),
                "step_id": step_id,
                "update": str(facts.index),
            },
        ))
    return tuple(traces)


class ReplayTimer:
    """Marks a monotonic clock around each update of a replay.

    It rides the ``on_update`` observation hook the replay driver already
    offers, so no execution module is patched and no execution ordering
    changes. The interval it reports is the time between consecutive
    marks: it therefore INCLUDES the driver's per-update corpus
    materialization, which is why
    :data:`MEASUREMENT_SOURCE_CYCLE_INTERVAL` says so instead of claiming
    a bare scheduler duration. The FIRST interval runs from
    :meth:`start` to the driver's first mark, so it additionally carries
    whatever one-time setup the driver did before that mark;
    :func:`measure_replay` labels it
    :data:`MEASUREMENT_SOURCE_CYCLE_INTERVAL_WITH_SETUP` for that
    reason."""

    def __init__(self, clock: Callable[[], int],
                 chain: Callable[[Any], None] | None = None) -> None:
        self._clock = clock
        self._chain = chain
        self._mark: int | None = None
        self.records: list[tuple[Any, int]] = []

    def start(self) -> None:
        self._mark = int(self._clock())

    def on_update(self, execution: Any) -> None:
        if self._mark is None:  # pragma: no cover - defensive
            _refuse(REFUSAL_ADAPTER_SURFACE, "$.timer",
                    "the timer was never started; an unmarked interval is "
                    "not a measurement")
        now = int(self._clock())
        elapsed = now - int(self._mark)
        self._mark = now
        if elapsed < 0:  # pragma: no cover - defensive
            _refuse(REFUSAL_NON_CANONICAL_VALUE, "$.timer",
                    "the injected clock went backwards; a negative "
                    "interval is not a measurement")
        self.records.append((execution, elapsed))
        if self._chain is not None:
            self._chain(execution)


@dataclass(frozen=True)
class MeasuredReplay:
    """A replay's own result, untouched, plus the two trace accounts.

    ``run`` is exactly what the replay driver returned. The wrapper adds
    records; it does not filter, reorder, or reinterpret them, which is
    what makes "the wrapped run's verdicts equal the unwrapped run's" a
    property the DB witness can assert rather than assume."""

    run: Any
    cycle_traces: ActionTraceSet
    step_traces: ActionTraceSet


def measure_replay(
    replay: Callable[..., Any],
    *,
    binding: ReproducibilityBinding,
    trace_set_id: str,
    epoch: int = 0,
    clock: Callable[[], int] | None = None,
    declared_resources: Sequence[str] = PARITY_RESOURCES,
    duration_unit: str = DURATION_UNIT_NANOSECONDS,
    cost_unit: str = COST_UNIT_EXECUTOR_STEPS,
    on_update: Callable[[Any], None] | None = None,
    first_interval_measurement_source: str = (
        MEASUREMENT_SOURCE_CYCLE_INTERVAL_WITH_SETUP),
    **replay_kwargs: Any,
) -> MeasuredReplay:
    """Run ``replay`` unchanged and derive per-action traces from it.

    ``replay`` is the existing driver (``parity.replay_mode``) and
    ``replay_kwargs`` are its own arguments, passed through verbatim. The
    only thing this function contributes is an ``on_update`` observer —
    a hook the driver already publishes — and a clock read on either side
    of each update. A caller's own ``on_update`` is chained, not
    replaced.

    ``clock`` is injectable so a test is deterministic; the production
    default is :func:`time.monotonic_ns`.

    The FIRST interval is not the same instrument as the rest. The clock
    is marked before the driver is called, and the driver does its
    one-time setup — for ``parity.replay_mode``, materializing the whole
    history's base corpus — before it reaches its first ``on_update``
    mark. That work is genuinely inside the elapsed interval, and the
    seam this module is allowed to observe cannot separate it without
    changing the driver, so it is DISCLOSED instead of hidden: trace 0
    declares :data:`MEASUREMENT_SOURCE_CYCLE_INTERVAL_WITH_SETUP`, which
    names the extra term, and :func:`contract_action_fields` refuses to
    recalibrate an estimate from it unless the caller asks by name. Two
    intervals measured by different instruments never share one
    label."""
    if "on_update" in replay_kwargs:  # pragma: no cover - defensive
        _refuse(REFUSAL_ADAPTER_SURFACE, "$.replay_kwargs.on_update",
                "pass the caller's observer as `on_update`; it is chained "
                "rather than overwritten")
    if clock is None:
        import time

        clock = time.monotonic_ns
    timer = ReplayTimer(clock, chain=on_update)
    timer.start()
    run = replay(on_update=timer.on_update, **replay_kwargs)

    cycle: list[ActionTrace] = []
    steps: list[ActionTrace] = []
    for position, (execution, elapsed) in enumerate(timer.records):
        cycle.append(cycle_trace_from_execution(
            execution, realized_duration=elapsed, binding=binding,
            declared_resources=declared_resources,
            duration_unit=duration_unit, cost_unit=cost_unit,
            measurement_source=(
                first_interval_measurement_source if position == 0
                else MEASUREMENT_SOURCE_CYCLE_INTERVAL
            ),
        ))
        steps.extend(step_traces_from_execution(
            execution, binding=binding,
            declared_resources=declared_resources,
            duration_unit=duration_unit, cost_unit=cost_unit,
        ))
    return MeasuredReplay(
        run=run,
        cycle_traces=ActionTraceSet(
            schema_version=MEASUREMENT_SCHEMA_VERSION,
            trace_set_id=f"{trace_set_id}/{GRANULARITY_PUBLISH_CYCLE}",
            epoch=epoch,
            granularity=GRANULARITY_PUBLISH_CYCLE,
            cost_unit=cost_unit,
            duration_unit=duration_unit,
            declared_resources=tuple(declared_resources),
            traces=tuple(cycle),
        ),
        step_traces=ActionTraceSet(
            schema_version=MEASUREMENT_SCHEMA_VERSION,
            trace_set_id=f"{trace_set_id}/{GRANULARITY_EXECUTOR_STEP}",
            epoch=epoch,
            granularity=GRANULARITY_EXECUTOR_STEP,
            cost_unit=cost_unit,
            duration_unit=duration_unit,
            declared_resources=tuple(declared_resources),
            traces=tuple(steps),
        ),
    )
