"""Aggregate extension resource and provider-effect admission."""

from __future__ import annotations

import hashlib
import json
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from .extension_broker import (
    ExtensionBrokerResult,
    ExtensionPackagePlan,
    ExtensionPlanEntry,
    _execute_extension_unchecked,
    verify_extension_package_plan,
)


RESOURCE_UNITS: Mapping[str, str] = {
    "cpu": "millicore",
    "memory": "byte",
    "concurrency": "slot",
    "workers": "worker",
    "storage": "byte",
    "network": "byte",
    "filesystem": "byte",
    "model_spend": "micro_usd",
    "connections": "connection",
    "credentials": "credential",
}
OBSERVATIONAL_PHASES = frozenset(
    {"metadata-plan", "discovery", "setup-proposal", "preflight", "health"}
)
ExtensionPhase = Literal[
    "metadata-plan",
    "discovery",
    "setup-proposal",
    "preflight",
    "health",
    "apply",
]


class ExtensionAdmissionError(ValueError):
    """Stable pre-mutation admission refusal."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        dimension: str | None = None,
        expected: int | str | None = None,
        observed: int | str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.dimension = dimension
        self.expected = expected
        self.observed = observed


@dataclass(frozen=True, slots=True)
class ResourceQuantity:
    dimension: str
    amount: int
    unit: str
    owner: str

    def __post_init__(self) -> None:
        expected_unit = RESOURCE_UNITS.get(self.dimension)
        if expected_unit is None:
            raise ExtensionAdmissionError(
                "extension_resource_unknown",
                f"unknown resource dimension {self.dimension!r}",
                dimension=self.dimension,
            )
        if self.unit != expected_unit:
            raise ExtensionAdmissionError(
                "extension_resource_unit_invalid",
                f"{self.dimension} must use unit {expected_unit!r}",
                dimension=self.dimension,
                expected=expected_unit,
                observed=self.unit,
            )
        if isinstance(self.amount, bool) or not isinstance(self.amount, int) or self.amount < 0:
            raise ExtensionAdmissionError(
                "extension_resource_amount_invalid",
                f"{self.dimension} amount must be a non-negative integer",
                dimension=self.dimension,
                observed=self.amount,
            )
        _identifier(self.owner, "resource owner")


@dataclass(frozen=True, slots=True)
class OwnedCapability:
    name: str
    owner: str

    def __post_init__(self) -> None:
        _identifier(self.name, "capability")
        _identifier(self.owner, "capability owner")


@dataclass(frozen=True, slots=True)
class OwnedProviderEffect:
    effect: Literal["observe", "write"]
    owner: str

    def __post_init__(self) -> None:
        _identifier(self.owner, "provider effect owner")


@dataclass(frozen=True, slots=True)
class ExtensionRequirements:
    owner: str
    quantities: tuple[ResourceQuantity, ...]
    capabilities: tuple[OwnedCapability, ...]
    provider_effects: tuple[OwnedProviderEffect, ...]

    def __post_init__(self) -> None:
        _identifier(self.owner, "extension owner")
        dimensions = [item.dimension for item in self.quantities]
        missing = sorted(set(RESOURCE_UNITS) - set(dimensions))
        unknown = sorted(set(dimensions) - set(RESOURCE_UNITS))
        duplicates = sorted({item for item in dimensions if dimensions.count(item) > 1})
        if missing or unknown or duplicates:
            raise ExtensionAdmissionError(
                "extension_resource_dimensions_invalid",
                "extension requirements must declare every resource dimension exactly once",
                expected=",".join(sorted(RESOURCE_UNITS)),
                observed=",".join(dimensions),
            )
        owned = [
            item.owner
            for item in (*self.quantities, *self.capabilities, *self.provider_effects)
        ]
        if any(owner != self.owner for owner in owned):
            raise ExtensionAdmissionError(
                "extension_resource_owner_mismatch",
                "every requirement must name its extension id as owner",
                expected=self.owner,
            )
        capability_names = [item.name for item in self.capabilities]
        if len(capability_names) != len(set(capability_names)):
            raise ExtensionAdmissionError(
                "extension_capability_duplicate",
                "extension capability requirements must be unique",
            )
        effects = [item.effect for item in self.provider_effects]
        if not effects or len(effects) != len(set(effects)):
            raise ExtensionAdmissionError(
                "extension_provider_effects_invalid",
                "provider effects must be non-empty and unique",
            )


@dataclass(frozen=True, slots=True)
class ExtensionCapacityPolicy:
    owner: str
    snapshot_id: str
    observed_at: datetime
    budgets: tuple[ResourceQuantity, ...]
    capabilities: tuple[str, ...]
    provider_effects: tuple[Literal["observe", "write"], ...]

    def __post_init__(self) -> None:
        _identifier(self.owner, "policy owner")
        _identifier(self.snapshot_id, "capacity snapshot id")
        _utc(self.observed_at)
        dimensions = [item.dimension for item in self.budgets]
        if set(dimensions) != set(RESOURCE_UNITS) or len(dimensions) != len(RESOURCE_UNITS):
            raise ExtensionAdmissionError(
                "extension_capacity_dimensions_invalid",
                "capacity policy must declare every resource dimension exactly once",
            )
        if any(item.owner != self.owner for item in self.budgets):
            raise ExtensionAdmissionError(
                "extension_capacity_owner_mismatch",
                "every capacity budget must name the policy owner",
            )
        if len(self.capabilities) != len(set(self.capabilities)):
            raise ExtensionAdmissionError(
                "extension_capacity_capabilities_invalid",
                "policy capabilities must be unique",
            )
        if not self.provider_effects or len(self.provider_effects) != len(set(self.provider_effects)):
            raise ExtensionAdmissionError(
                "extension_capacity_effects_invalid",
                "policy provider effects must be non-empty and unique",
            )

    @property
    def digest(self) -> str:
        return _digest(
            {
                "owner": self.owner,
                "snapshot_id": self.snapshot_id,
                "observed_at": _utc(self.observed_at).isoformat(),
                "budgets": [_quantity_payload(item) for item in sorted(self.budgets, key=lambda q: q.dimension)],
                "capabilities": sorted(self.capabilities),
                "provider_effects": sorted(self.provider_effects),
            }
        )


@dataclass(frozen=True, slots=True)
class ExtensionAdmissionPlan:
    entries: tuple[tuple[str, str, str], ...]
    aggregate: tuple[ResourceQuantity, ...]
    capabilities: tuple[str, ...]
    provider_effects: tuple[Literal["observe", "write"], ...]
    policy_digest: str
    capacity_snapshot_id: str
    created_at: datetime
    plan_digest: str


@dataclass(frozen=True, slots=True)
class ExtensionAdmissionReceipt:
    reservation_id: str
    plan_digest: str
    capacity_snapshot_id: str
    admitted_at: datetime
    phase: ExtensionPhase


class ExtensionReservationLedger:
    """Injected reservation authority with idempotent release/retry behavior."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._active: dict[str, dict[str, int]] = {}
        self.acquire_count = 0
        self.release_count = 0

    @property
    def active_count(self) -> int:
        with self._lock:
            return len(self._active)

    @property
    def active_totals(self) -> Mapping[str, int]:
        with self._lock:
            totals = {dimension: 0 for dimension in RESOURCE_UNITS}
            for demand in self._active.values():
                for dimension, amount in demand.items():
                    totals[dimension] += amount
            return totals

    def reserve(
        self,
        reservation_id: str,
        demand: Mapping[str, int],
        budgets: Mapping[str, int],
    ) -> "ExtensionAdmissionLease":
        with self._lock:
            if reservation_id in self._active:
                raise ExtensionAdmissionError(
                    "extension_reservation_active",
                    "an active reservation already exists for this admission plan",
                )
            totals = dict(self.active_totals)
            for dimension in RESOURCE_UNITS:
                observed = totals[dimension] + demand[dimension]
                if observed > budgets[dimension]:
                    raise ExtensionAdmissionError(
                        "extension_capacity_exceeded",
                        f"aggregate {dimension} demand exceeds available capacity",
                        dimension=dimension,
                        expected=budgets[dimension],
                        observed=observed,
                    )
            self._active[reservation_id] = dict(demand)
            self.acquire_count += 1
            return ExtensionAdmissionLease(self, reservation_id)

    def release(self, reservation_id: str) -> None:
        with self._lock:
            if self._active.pop(reservation_id, None) is not None:
                self.release_count += 1

    def is_active(self, reservation_id: str) -> bool:
        with self._lock:
            return reservation_id in self._active


class ExtensionAdmissionLease:
    def __init__(self, ledger: ExtensionReservationLedger, reservation_id: str) -> None:
        self._ledger = ledger
        self.reservation_id = reservation_id

    def release(self) -> None:
        self._ledger.release(self.reservation_id)

    def __enter__(self) -> "ExtensionAdmissionLease":
        return self

    def __exit__(self, _type: Any, _value: Any, _traceback: Any) -> None:
        self.release()


def parse_extension_requirements(entry: ExtensionPlanEntry) -> ExtensionRequirements:
    raw = entry.requirements
    if _digest(raw) != entry.requirements_digest:
        raise ExtensionAdmissionError(
            "extension_requirements_digest_mismatch",
            "extension resource declarations changed after metadata planning",
        )
    if set(raw) != {"quantities", "capabilities", "provider_effects"}:
        raise ExtensionAdmissionError(
            "extension_requirements_fields_invalid",
            "requirements must contain quantities, capabilities, and provider_effects",
        )
    quantities_raw = _list_of_objects(raw["quantities"], "quantities")
    capabilities_raw = _list_of_objects(raw["capabilities"], "capabilities")
    effects_raw = _list_of_objects(raw["provider_effects"], "provider_effects")
    quantities = tuple(
        ResourceQuantity(
            dimension=_field(item, "dimension", str),
            amount=_field(item, "amount", int),
            unit=_field(item, "unit", str),
            owner=_field(item, "owner", str),
        )
        for item in quantities_raw
    )
    capabilities = tuple(
        OwnedCapability(
            name=_field(item, "name", str),
            owner=_field(item, "owner", str),
        )
        for item in capabilities_raw
    )
    effects = tuple(
        OwnedProviderEffect(
            effect=_provider_effect(_field(item, "effect", str)),
            owner=_field(item, "owner", str),
        )
        for item in effects_raw
    )
    requirements = ExtensionRequirements(
        owner=entry.extension_id,
        quantities=quantities,
        capabilities=capabilities,
        provider_effects=effects,
    )
    declared = tuple(sorted(item.name for item in requirements.capabilities))
    if declared != entry.capabilities:
        raise ExtensionAdmissionError(
            "extension_capability_contract_mismatch",
            "entrypoint and resource capability declarations must match",
            expected=",".join(entry.capabilities),
            observed=",".join(declared),
        )
    return requirements


def build_extension_admission_plan(
    selections: Sequence[tuple[ExtensionPackagePlan, str]],
    *,
    policy: ExtensionCapacityPolicy,
    created_at: datetime,
) -> ExtensionAdmissionPlan:
    """Aggregate metadata declarations without importing or launching extensions."""

    if not selections:
        raise ExtensionAdmissionError(
            "extension_selection_empty",
            "at least one extension must be selected for admission",
        )
    demands = {dimension: 0 for dimension in RESOURCE_UNITS}
    capabilities: set[str] = set()
    effects: set[Literal["observe", "write"]] = set()
    entry_pins: list[tuple[str, str, str]] = []
    for package, extension_id in selections:
        try:
            package = verify_extension_package_plan(package)
        except ValueError as exc:
            raise ExtensionAdmissionError(
                getattr(exc, "code", "extension_package_plan_unverified"),
                str(exc),
            ) from exc
        entry = next((item for item in package.entries if item.extension_id == extension_id), None)
        if entry is None:
            raise ExtensionAdmissionError(
                "extension_id_unregistered",
                f"extension id {extension_id!r} is absent from its package plan",
            )
        requirements = parse_extension_requirements(entry)
        for quantity in requirements.quantities:
            demands[quantity.dimension] += quantity.amount
        capabilities.update(item.name for item in requirements.capabilities)
        effects.update(item.effect for item in requirements.provider_effects)
        entry_pins.append((package.package_digest, package.plan_digest, extension_id))
    aggregate = tuple(
        ResourceQuantity(
            dimension=dimension,
            amount=demands[dimension],
            unit=unit,
            owner=policy.owner,
        )
        for dimension, unit in RESOURCE_UNITS.items()
    )
    payload = {
        "entries": sorted(entry_pins),
        "aggregate": [_quantity_payload(item) for item in aggregate],
        "capabilities": sorted(capabilities),
        "provider_effects": sorted(effects),
        "policy_digest": policy.digest,
        "capacity_snapshot_id": policy.snapshot_id,
        "created_at": _utc(created_at).isoformat(),
    }
    return ExtensionAdmissionPlan(
        entries=tuple(sorted(entry_pins)),
        aggregate=aggregate,
        capabilities=tuple(sorted(capabilities)),
        provider_effects=tuple(sorted(effects)),
        policy_digest=policy.digest,
        capacity_snapshot_id=policy.snapshot_id,
        created_at=_utc(created_at),
        plan_digest=_digest(payload),
    )


def admit_extension_plan(
    plan: ExtensionAdmissionPlan,
    *,
    policy: ExtensionCapacityPolicy,
    ledger: ExtensionReservationLedger,
    now: datetime,
    phase: ExtensionPhase,
    current_packages: Mapping[str, ExtensionPackagePlan],
    max_age: timedelta = timedelta(minutes=5),
) -> tuple[ExtensionAdmissionReceipt, ExtensionAdmissionLease]:
    """Validate every guard, then acquire one aggregate reservation atomically."""

    _validate_admission_plan_shape(plan, policy=policy)
    expected_plan_digest = _admission_plan_digest(plan)
    if plan.plan_digest != expected_plan_digest:
        raise ExtensionAdmissionError(
            "extension_admission_plan_digest_mismatch",
            "extension admission plan changed after approval",
            expected=expected_plan_digest,
            observed=plan.plan_digest,
        )
    now_utc = _utc(now)
    if _utc(plan.created_at) > now_utc:
        raise ExtensionAdmissionError(
            "extension_admission_plan_from_future",
            "extension admission plan issue time is in the future",
        )
    if now_utc - _utc(plan.created_at) > max_age:
        raise ExtensionAdmissionError(
            "extension_admission_plan_stale",
            "extension admission plan is stale",
        )
    if _utc(policy.observed_at) > now_utc:
        raise ExtensionAdmissionError(
            "extension_capacity_snapshot_from_future",
            "extension capacity snapshot time is in the future",
        )
    if now_utc - _utc(policy.observed_at) > max_age:
        raise ExtensionAdmissionError(
            "extension_capacity_snapshot_stale",
            "extension capacity snapshot is stale",
        )
    if plan.policy_digest != policy.digest or plan.capacity_snapshot_id != policy.snapshot_id:
        raise ExtensionAdmissionError(
            "extension_capacity_policy_mismatch",
            "admission plan does not match current explicit capacity policy",
        )
    canonical_selections: list[tuple[ExtensionPackagePlan, str]] = []
    for package_digest, package_plan_digest, extension_id in plan.entries:
        current_package = current_packages.get(extension_id)
        if current_package is None:
            raise ExtensionAdmissionError(
                "extension_package_plan_stale",
                f"package plan changed for extension {extension_id!r}",
            )
        try:
            current_package = verify_extension_package_plan(current_package)
        except ValueError as exc:
            raise ExtensionAdmissionError(
                getattr(exc, "code", "extension_package_plan_unverified"),
                str(exc),
            ) from exc
        if (
            current_package.package_digest != package_digest
            or current_package.plan_digest != package_plan_digest
        ):
            raise ExtensionAdmissionError(
                "extension_package_plan_stale",
                f"package plan changed for extension {extension_id!r}",
            )
        canonical_selections.append((current_package, extension_id))
    canonical_plan = build_extension_admission_plan(
        canonical_selections,
        policy=policy,
        created_at=plan.created_at,
    )
    if canonical_plan != plan:
        raise ExtensionAdmissionError(
            "extension_admission_content_mismatch",
            "admission aggregate does not match canonical package requirements",
        )
    undeclared_capabilities = sorted(set(plan.capabilities) - set(policy.capabilities))
    if undeclared_capabilities:
        raise ExtensionAdmissionError(
            "extension_capability_not_granted",
            "capacity policy does not grant: " + ", ".join(undeclared_capabilities),
        )
    undeclared_effects = sorted(set(plan.provider_effects) - set(policy.provider_effects))
    if undeclared_effects:
        raise ExtensionAdmissionError(
            "extension_provider_effect_not_granted",
            "capacity policy does not grant provider effects: " + ", ".join(undeclared_effects),
        )
    if phase in OBSERVATIONAL_PHASES and set(plan.provider_effects) != {"observe"}:
        raise ExtensionAdmissionError(
            "extension_observational_write_refused",
            f"{phase} admits the observe provider effect only",
        )
    demand = {item.dimension: item.amount for item in plan.aggregate}
    budgets = {item.dimension: item.amount for item in policy.budgets}
    reservation_id = _digest(
        {
            "plan_digest": plan.plan_digest,
            "capacity_snapshot_id": policy.snapshot_id,
            "phase": phase,
        }
    )
    lease = ledger.reserve(reservation_id, demand, budgets)
    return (
        ExtensionAdmissionReceipt(
            reservation_id=reservation_id,
            plan_digest=plan.plan_digest,
            capacity_snapshot_id=policy.snapshot_id,
            admitted_at=now_utc,
            phase=phase,
        ),
        lease,
    )


def execute_admitted_extension(
    package: ExtensionPackagePlan,
    admission_plan: ExtensionAdmissionPlan,
    receipt: ExtensionAdmissionReceipt,
    ledger: ExtensionReservationLedger,
    *,
    extension_id: str,
    binding_id: str,
    capabilities: Sequence[str],
    payload: Mapping[str, Any],
    publish: Any = None,
    timeout_seconds: float = 60.0,
) -> ExtensionBrokerResult:
    """Execute only while the exact aggregate admission reservation is active."""

    if receipt.plan_digest != admission_plan.plan_digest:
        raise ExtensionAdmissionError(
            "extension_execution_admission_mismatch",
            "execution receipt does not match the aggregate admission plan",
        )
    if not ledger.is_active(receipt.reservation_id):
        raise ExtensionAdmissionError(
            "extension_execution_reservation_inactive",
            "extension execution requires an active aggregate reservation",
        )
    if receipt.phase in OBSERVATIONAL_PHASES and set(admission_plan.provider_effects) != {"observe"}:
        raise ExtensionAdmissionError(
            "extension_execution_phase_mismatch",
            f"{receipt.phase} execution permits the observe provider effect only",
        )
    expected_pin = (package.package_digest, package.plan_digest, extension_id)
    if expected_pin not in admission_plan.entries:
        raise ExtensionAdmissionError(
            "extension_execution_not_admitted",
            f"extension {extension_id!r} is absent from the aggregate admission plan",
        )
    return _execute_extension_unchecked(
        package,
        extension_id=extension_id,
        binding_id=binding_id,
        capabilities=capabilities,
        payload=payload,
        publish=publish,
        timeout_seconds=timeout_seconds,
    )


def _validate_admission_plan_shape(
    plan: ExtensionAdmissionPlan,
    *,
    policy: ExtensionCapacityPolicy,
) -> None:
    dimensions = [item.dimension for item in plan.aggregate]
    if set(dimensions) != set(RESOURCE_UNITS) or len(dimensions) != len(RESOURCE_UNITS):
        raise ExtensionAdmissionError(
            "extension_admission_dimensions_invalid",
            "admission plan must contain every aggregate resource dimension exactly once",
        )
    for item in plan.aggregate:
        ResourceQuantity(
            dimension=item.dimension,
            amount=item.amount,
            unit=item.unit,
            owner=item.owner,
        )
        if item.owner != policy.owner:
            raise ExtensionAdmissionError(
                "extension_admission_owner_mismatch",
                "aggregate admission quantities must name the capacity policy owner",
            )
    extension_ids = [item[2] for item in plan.entries]
    if not extension_ids or len(extension_ids) != len(set(extension_ids)):
        raise ExtensionAdmissionError(
            "extension_admission_entries_invalid",
            "admission plan entries must be non-empty and unique by extension id",
        )
    if len(plan.capabilities) != len(set(plan.capabilities)):
        raise ExtensionAdmissionError(
            "extension_admission_capabilities_invalid",
            "admission plan capabilities must be unique",
        )
    for capability in plan.capabilities:
        _identifier(capability, "admission capability")
    if not plan.provider_effects or any(
        effect not in {"observe", "write"} for effect in plan.provider_effects
    ) or len(plan.provider_effects) != len(set(plan.provider_effects)):
        raise ExtensionAdmissionError(
            "extension_admission_effects_invalid",
            "admission plan provider effects must be known, non-empty, and unique",
        )


def _admission_plan_digest(plan: ExtensionAdmissionPlan) -> str:
    return _digest(
        {
            "entries": list(plan.entries),
            "aggregate": [_quantity_payload(item) for item in plan.aggregate],
            "capabilities": list(plan.capabilities),
            "provider_effects": list(plan.provider_effects),
            "policy_digest": plan.policy_digest,
            "capacity_snapshot_id": plan.capacity_snapshot_id,
            "created_at": _utc(plan.created_at).isoformat(),
        }
    )


def _quantity_payload(value: ResourceQuantity) -> dict[str, Any]:
    return {
        "dimension": value.dimension,
        "amount": value.amount,
        "unit": value.unit,
        "owner": value.owner,
    }


def _provider_effect(value: str) -> Literal["observe", "write"]:
    if value not in {"observe", "write"}:
        raise ExtensionAdmissionError(
            "extension_provider_effect_unknown",
            f"unknown provider effect {value!r}",
        )
    return value  # type: ignore[return-value]


def _list_of_objects(value: Any, field: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ExtensionAdmissionError(
            "extension_requirements_malformed",
            f"{field} must be a list of objects",
        )
    return value


def _field(value: Mapping[str, Any], field: str, expected: type[Any]) -> Any:
    expected_keys = {"dimension", "amount", "unit", "owner"} if "dimension" in value else (
        {"name", "owner"} if "name" in value else {"effect", "owner"}
    )
    if set(value) != expected_keys or field not in value or not isinstance(value[field], expected):
        raise ExtensionAdmissionError(
            "extension_requirements_malformed",
            f"requirement field {field!r} is missing or malformed",
        )
    return value[field]


def _identifier(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or any(char.isspace() for char in value):
        raise ExtensionAdmissionError(
            "extension_identifier_invalid",
            f"{field} must be a non-empty string without whitespace",
        )
    return value


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ExtensionAdmissionError(
            "extension_timestamp_invalid",
            "extension admission timestamps must be timezone-aware",
        )
    return value.astimezone(timezone.utc)


def _digest(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


__all__ = [
    "OBSERVATIONAL_PHASES",
    "RESOURCE_UNITS",
    "ExtensionAdmissionError",
    "ExtensionAdmissionLease",
    "ExtensionAdmissionPlan",
    "ExtensionAdmissionReceipt",
    "ExtensionCapacityPolicy",
    "ExtensionRequirements",
    "ExtensionReservationLedger",
    "OwnedCapability",
    "OwnedProviderEffect",
    "ResourceQuantity",
    "admit_extension_plan",
    "build_extension_admission_plan",
    "execute_admitted_extension",
    "parse_extension_requirements",
]
