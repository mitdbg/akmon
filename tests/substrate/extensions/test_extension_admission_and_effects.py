"""Aggregate extension resource/effect admission contract tests."""

from __future__ import annotations

import json
import shutil
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.extension_admission import (
    RESOURCE_UNITS,
    ExtensionAdmissionError,
    ExtensionCapacityPolicy,
    ExtensionReservationLedger,
    ResourceQuantity,
    admit_extension_plan,
    build_extension_admission_plan,
    execute_admitted_extension,
)
from okg.substrate.extension_admission import _admission_plan_digest
from okg.substrate.extension_broker import (
    ExtensionBoundaryError,
    extension_package_plan_digest,
    plan_extension_package,
)


FIXTURES = Path(__file__).parents[2] / "fixtures" / "deployment_products"
PACKAGE_FIXTURE = FIXTURES / "external_extension"
POLICY_FIXTURE = FIXTURES / "extension_admission" / "policy.json"
NOW = datetime(2026, 8, 18, 12, 0, tzinfo=timezone.utc)
SELECTED_IDS = (
    "fixture.connector",
    "fixture.enricher",
    "fixture.tool",
    "fixture.network-probe",
    "fixture.filesystem-probe",
)


def _policy(*, observed_at: datetime = NOW) -> ExtensionCapacityPolicy:
    raw = json.loads(POLICY_FIXTURE.read_text())
    return ExtensionCapacityPolicy(
        owner=raw["owner"],
        snapshot_id=raw["snapshot_id"],
        observed_at=observed_at,
        budgets=tuple(ResourceQuantity(**item) for item in raw["budgets"]),
        capabilities=tuple(raw["capabilities"]),
        provider_effects=tuple(raw["provider_effects"]),
    )


def _package(root: Path = PACKAGE_FIXTURE):
    return plan_extension_package(root)


def _selections(package: Any) -> list[tuple[Any, str]]:
    return [(package, extension_id) for extension_id in SELECTED_IDS]


def _current_packages(
    package: Any, ids: tuple[str, ...] = SELECTED_IDS
) -> dict[str, Any]:
    return {extension_id: package for extension_id in ids}


def _plan(package: Any, policy: ExtensionCapacityPolicy, *, created_at: datetime = NOW):
    return build_extension_admission_plan(
        _selections(package),
        policy=policy,
        created_at=created_at,
    )


def _copy_package(tmp_path: Path) -> Path:
    target = tmp_path / "external-package"
    shutil.copytree(PACKAGE_FIXTURE, target)
    return target


def test_metadata_aggregate_declares_every_dimension_with_units_and_owners() -> None:
    package = _package()
    policy = _policy()
    plan = _plan(package, policy)
    aggregate = {item.dimension: item for item in plan.aggregate}

    assert set(aggregate) == set(RESOURCE_UNITS)
    assert {name: item.unit for name, item in aggregate.items()} == dict(RESOURCE_UNITS)
    assert all(item.owner == policy.owner for item in aggregate.values())
    assert {name: item.amount for name, item in aggregate.items()} == {
        "cpu": 195,
        "memory": 60_817_408,
        "concurrency": 5,
        "workers": 1,
        "storage": 1024,
        "network": 3584,
        "filesystem": 1024,
        "model_spend": 1000,
        "connections": 2,
        "credentials": 2,
    }
    assert plan.capabilities == (
        "filesystem",
        "graph.output.propose",
        "network",
        "provider.metadata.read",
        "provider.source.read",
    )
    assert plan.provider_effects == ("observe",)
    assert all(
        value.startswith("sha256:")
        for value in (
            package.package_digest,
            package.dependency_digest,
            package.entrypoint_digest,
            package.provenance_digest,
            package.sbom_digest,
            plan.policy_digest,
            plan.plan_digest,
        )
    )


def test_caller_constructed_source_path_plan_is_refused_before_admission() -> None:
    package = _package()
    forged_entry = replace(package.entries[0], source_path="../uninspected.py")
    forged = replace(
        package, entries=(forged_entry, *package.entries[1:]), plan_digest=""
    )
    forged = replace(forged, plan_digest=extension_package_plan_digest(forged))

    with pytest.raises(ExtensionAdmissionError) as caught:
        build_extension_admission_plan(
            [(forged, forged_entry.extension_id)],
            policy=_policy(),
            created_at=NOW,
        )
    assert caught.value.code == "extension_package_plan_unverified"


def test_in_budget_plan_reserves_only_after_all_guards_and_releases_idempotently() -> (
    None
):
    package = _package()
    policy = _policy()
    plan = _plan(package, policy)
    ledger = ExtensionReservationLedger()

    receipt, lease = admit_extension_plan(
        plan,
        policy=policy,
        ledger=ledger,
        now=NOW,
        phase="preflight",
        current_packages=_current_packages(package),
    )
    assert receipt.plan_digest == plan.plan_digest
    assert receipt.capacity_snapshot_id == policy.snapshot_id
    assert receipt.phase == "preflight"
    assert ledger.active_count == 1
    assert ledger.acquire_count == 1
    lease.release()
    lease.release()
    assert ledger.active_count == 0
    assert ledger.release_count == 1


@pytest.mark.parametrize("dimension", tuple(RESOURCE_UNITS))
def test_each_over_budget_dimension_refuses_before_any_mutation(dimension: str) -> None:
    package = _package()
    base_policy = _policy()
    plan = _plan(package, base_policy)
    demand = {item.dimension: item.amount for item in plan.aggregate}
    budgets = tuple(
        replace(item, amount=max(0, demand[dimension] - 1))
        if item.dimension == dimension
        else item
        for item in base_policy.budgets
    )
    constrained = replace(base_policy, budgets=budgets)
    constrained_plan = _plan(package, constrained)
    ledger = ExtensionReservationLedger()
    counters = {
        "process": 0,
        "database": 0,
        "graph": 0,
        "provider": 0,
        "generation": 0,
    }

    with pytest.raises(ExtensionAdmissionError) as caught:
        admit_extension_plan(
            constrained_plan,
            policy=constrained,
            ledger=ledger,
            now=NOW,
            phase="apply",
            current_packages=_current_packages(package),
        )
    assert caught.value.code == "extension_capacity_exceeded"
    assert caught.value.dimension == dimension
    assert ledger.active_count == ledger.acquire_count == ledger.release_count == 0
    assert counters == {key: 0 for key in counters}


@pytest.mark.parametrize(
    "mutation,code",
    [
        ("missing", "extension_resource_dimensions_invalid"),
        ("unknown", "extension_resource_unknown"),
        ("unit", "extension_resource_unit_invalid"),
        ("owner", "extension_resource_owner_mismatch"),
    ],
)
def test_missing_unknown_malformed_or_unowned_metadata_refuses_before_reservation(
    tmp_path: Path,
    mutation: str,
    code: str,
) -> None:
    root = _copy_package(tmp_path)
    descriptor_path = root / "okg-extension.json"
    descriptor = json.loads(descriptor_path.read_text())
    quantities = descriptor["entries"][0]["requirements"]["quantities"]
    if mutation == "missing":
        quantities.pop()
    elif mutation == "unknown":
        quantities[0]["dimension"] = "gpu"
    elif mutation == "unit":
        quantities[0]["unit"] = "core"
    else:
        quantities[0]["owner"] = "another-extension"
    descriptor_path.write_text(json.dumps(descriptor, sort_keys=True))
    package = _package(root)
    policy = _policy()
    ledger = ExtensionReservationLedger()

    with pytest.raises(ExtensionAdmissionError) as caught:
        build_extension_admission_plan(
            [(package, "fixture.connector")],
            policy=policy,
            created_at=NOW,
        )
    assert caught.value.code == code
    assert ledger.active_count == ledger.acquire_count == 0


def test_stale_tampered_package_or_ungranted_capability_refuses_before_reservation() -> (
    None
):
    package = _package()
    policy = _policy()
    ledger = ExtensionReservationLedger()

    stale = _plan(package, policy, created_at=NOW - timedelta(minutes=6))
    with pytest.raises(ExtensionAdmissionError) as caught:
        admit_extension_plan(
            stale,
            policy=policy,
            ledger=ledger,
            now=NOW,
            phase="apply",
            current_packages=_current_packages(package),
        )
    assert caught.value.code == "extension_admission_plan_stale"

    plan = _plan(package, policy)
    with pytest.raises(ExtensionAdmissionError) as caught:
        admit_extension_plan(
            replace(plan, plan_digest="sha256:" + "0" * 64),
            policy=policy,
            ledger=ledger,
            now=NOW,
            phase="apply",
            current_packages=_current_packages(package),
        )
    assert caught.value.code == "extension_admission_plan_digest_mismatch"

    with pytest.raises(ExtensionAdmissionError) as caught:
        admit_extension_plan(
            plan,
            policy=policy,
            ledger=ledger,
            now=NOW,
            phase="apply",
            current_packages={
                extension_id: current
                for extension_id, current in _current_packages(package).items()
                if extension_id != "fixture.connector"
            },
        )
    assert caught.value.code == "extension_package_plan_stale"

    restricted = replace(
        policy,
        capabilities=tuple(
            item for item in policy.capabilities if item != "provider.source.read"
        ),
    )
    restricted_plan = _plan(package, restricted)
    with pytest.raises(ExtensionAdmissionError) as caught:
        admit_extension_plan(
            restricted_plan,
            policy=restricted,
            ledger=ledger,
            now=NOW,
            phase="apply",
            current_packages=_current_packages(package),
        )
    assert caught.value.code == "extension_capability_not_granted"
    assert ledger.active_count == ledger.acquire_count == 0


def test_forged_resource_shape_and_mutated_package_metadata_refuse_before_reservation() -> (
    None
):
    package = _package()
    policy = _policy()
    plan = _plan(package, policy)
    ledger = ExtensionReservationLedger()
    forged_quantity = object.__new__(ResourceQuantity)
    object.__setattr__(forged_quantity, "dimension", "cpu")
    object.__setattr__(forged_quantity, "amount", -1)
    object.__setattr__(forged_quantity, "unit", "millicore")
    object.__setattr__(forged_quantity, "owner", policy.owner)
    forged = replace(
        plan,
        aggregate=(forged_quantity, *plan.aggregate[1:]),
    )
    with pytest.raises(ExtensionAdmissionError) as caught:
        admit_extension_plan(
            forged,
            policy=policy,
            ledger=ledger,
            now=NOW,
            phase="apply",
            current_packages=_current_packages(package),
        )
    assert caught.value.code == "extension_resource_amount_invalid"

    package.entries[0].requirements["quantities"][0]["amount"] = 0
    with pytest.raises(ExtensionAdmissionError) as caught:
        build_extension_admission_plan(
            _selections(package),
            policy=policy,
            created_at=NOW,
        )
    assert caught.value.code == "extension_package_plan_digest_mismatch"
    assert ledger.active_count == ledger.acquire_count == 0


def test_valid_digest_underreported_aggregate_refuses_before_reservation() -> None:
    package = _package()
    policy = _policy()
    plan = _plan(package, policy)
    understated = replace(
        plan,
        aggregate=tuple(replace(item, amount=0) for item in plan.aggregate),
    )
    understated = replace(understated, plan_digest=_admission_plan_digest(understated))
    ledger = ExtensionReservationLedger()

    with pytest.raises(ExtensionAdmissionError) as caught:
        admit_extension_plan(
            understated,
            policy=policy,
            ledger=ledger,
            now=NOW,
            phase="apply",
            current_packages=_current_packages(package),
        )
    assert caught.value.code == "extension_admission_content_mismatch"
    assert ledger.active_count == ledger.acquire_count == 0


def test_stale_capacity_and_observational_write_effect_refuse_with_zero_writes(
    tmp_path: Path,
) -> None:
    package = _package()
    stale_policy = _policy(observed_at=NOW - timedelta(minutes=6))
    stale_plan = _plan(package, stale_policy)
    ledger = ExtensionReservationLedger()
    with pytest.raises(ExtensionAdmissionError) as caught:
        admit_extension_plan(
            stale_plan,
            policy=stale_policy,
            ledger=ledger,
            now=NOW,
            phase="preflight",
            current_packages=_current_packages(package),
        )
    assert caught.value.code == "extension_capacity_snapshot_stale"

    root = _copy_package(tmp_path)
    descriptor_path = root / "okg-extension.json"
    descriptor = json.loads(descriptor_path.read_text())
    descriptor["entries"][0]["requirements"]["provider_effects"] = [
        {"effect": "write", "owner": "fixture.connector"}
    ]
    descriptor_path.write_text(json.dumps(descriptor, sort_keys=True))
    write_package = _package(root)
    policy = _policy()
    write_plan = build_extension_admission_plan(
        [(write_package, "fixture.connector")],
        policy=policy,
        created_at=NOW,
    )
    writes = {"provider": 0, "database": 0, "graph": 0, "generation": 0}
    with pytest.raises(ExtensionAdmissionError) as caught:
        admit_extension_plan(
            write_plan,
            policy=policy,
            ledger=ledger,
            now=NOW,
            phase="health",
            current_packages=_current_packages(write_package, ("fixture.connector",)),
        )
    assert caught.value.code == "extension_observational_write_refused"
    assert writes == {key: 0 for key in writes}
    assert ledger.active_count == ledger.acquire_count == 0


def test_crash_retry_releases_once_and_never_leaks_or_double_releases() -> None:
    package = _package()
    policy = _policy()
    plan = build_extension_admission_plan(
        [(package, "fixture.flaky")],
        policy=policy,
        created_at=NOW,
    )
    ledger = ExtensionReservationLedger()
    current = _current_packages(package, ("fixture.flaky",))

    receipt, lease = admit_extension_plan(
        plan,
        policy=policy,
        ledger=ledger,
        now=NOW,
        phase="apply",
        current_packages=current,
    )
    with pytest.raises(ExtensionBoundaryError) as caught:
        with lease:
            execute_admitted_extension(
                package,
                plan,
                receipt,
                ledger,
                extension_id="fixture.flaky",
                binding_id="flaky",
                capabilities=(),
                payload={"crash": True},
            )
    assert caught.value.code == "extension_execution_failed"
    assert ledger.active_count == 0
    assert ledger.acquire_count == 1
    assert ledger.release_count == 1

    retry_receipt, retry_lease = admit_extension_plan(
        plan,
        policy=policy,
        ledger=ledger,
        now=NOW + timedelta(seconds=1),
        phase="apply",
        current_packages=current,
    )
    with retry_lease:
        recovered = execute_admitted_extension(
            package,
            plan,
            retry_receipt,
            ledger,
            extension_id="fixture.flaky",
            binding_id="flaky",
            capabilities=(),
            payload={"crash": False},
        )
    assert recovered.batch.observations == {"recovered": True}
    retry_lease.release()
    assert ledger.active_count == 0
    assert ledger.acquire_count == 2
    assert ledger.release_count == 2


def test_context_manager_exception_also_releases_exactly_once() -> None:
    package = _package()
    policy = _policy()
    plan = _plan(package, policy)
    ledger = ExtensionReservationLedger()
    _receipt, lease = admit_extension_plan(
        plan,
        policy=policy,
        ledger=ledger,
        now=NOW,
        phase="apply",
        current_packages=_current_packages(package),
    )

    def crash_while_reserved() -> None:
        with lease:
            raise RuntimeError("simulated caller crash")

    with pytest.raises(RuntimeError):
        crash_while_reserved()
    assert ledger.active_count == 0
    assert ledger.acquire_count == 1
    assert ledger.release_count == 1


def test_extension_execution_requires_the_live_exact_admission_reservation() -> None:
    package = _package()
    policy = _policy()
    plan = build_extension_admission_plan(
        [(package, "fixture.tool")],
        policy=policy,
        created_at=NOW,
    )
    ledger = ExtensionReservationLedger()
    receipt, lease = admit_extension_plan(
        plan,
        policy=policy,
        ledger=ledger,
        now=NOW,
        phase="apply",
        current_packages=_current_packages(package, ("fixture.tool",)),
    )
    lease.release()

    with pytest.raises(ExtensionAdmissionError) as caught:
        execute_admitted_extension(
            package,
            plan,
            receipt,
            ledger,
            extension_id="fixture.tool",
            binding_id="tool",
            capabilities=("provider.metadata.read",),
            payload={"query": "must-not-run"},
        )
    assert caught.value.code == "extension_execution_reservation_inactive"
