from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
CHECKER = ROOT / "scripts/check_formal_fleet_capacity_reservation.py"
MODEL = ROOT / "formal/fleet_capacity_reservation/FleetCapacityReservation.tla"


def _checker():
    spec = importlib.util.spec_from_file_location("fleet_capacity_checker", CHECKER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_concurrent_reservations_never_oversubscribe() -> None:
    checker = _checker()
    initial = checker.ReservationState(capacity=5, standing_reserve=1, committed=1)
    first = checker.acquire(initial, "backup-a", 2)
    assert first is not None and first.total == 4
    second = checker.acquire(first, "restore-b", 2)
    assert second is None
    final = checker.acquire(first, "poller-c", 1)
    assert final is not None and final.total == final.capacity
    assert checker.acquire(final, "overflow", 1) is None


def test_crashed_reservation_reconciles_once() -> None:
    checker = _checker()
    initial = checker.ReservationState(capacity=4, standing_reserve=1)
    held = checker.acquire(initial, "backup-a", 2)
    assert held is not None
    reconciled = checker.reconcile_crash(held, "backup-a")
    replayed = checker.reconcile_crash(reconciled, "backup-a")
    assert reconciled == replayed
    assert reconciled.total == 1
    assert reconciled.reconciled == {"backup-a"}


def test_resource_domains_deduplicate_shared_capacity() -> None:
    checker = _checker()
    observations = [
        {
            "deployment": "alpha",
            "domain_id": "postgres-a",
            "capacity": 8,
            "observed_at": "t1",
        },
        {
            "deployment": "beta",
            "domain_id": "postgres-a",
            "capacity": 8,
            "observed_at": "t1",
        },
        {
            "deployment": "alpha",
            "domain_id": "filesystem-a",
            "capacity": 100,
            "observed_at": "t1",
        },
    ]
    domains = checker.deduplicate_resource_domains(observations)
    assert set(domains) == {"postgres-a", "filesystem-a"}
    assert sum(item["capacity"] for item in domains.values()) == 108
    conflicting = [
        *observations,
        {
            "deployment": "gamma",
            "domain_id": "postgres-a",
            "capacity": 9,
            "observed_at": "t1",
        },
    ]
    with pytest.raises(ValueError, match="conflicting observations"):
        checker.deduplicate_resource_domains(conflicting)

    text = MODEL.read_text(encoding="utf-8")
    assert "ResourceDomains ==" in text
    assert "Cardinality(ResourceDomains) = 1" in text


def _converted_permit(checker):
    ledger = checker.VectorReservationLedger(
        capacity=(("disk", 4), ("workers", 3)),
        committed=(("disk", 0), ("workers", 0)),
    )
    held = checker.acquire_vector_reservation(
        ledger,
        reservation_id="reservation-7",
        activation_epoch=7,
        activation_receipt="receipt-7",
        demand={"workers": 3, "disk": 4},
    )
    assert held is not None
    converted = checker.convert_held_reservation_to_permit(
        held,
        reservation_id="reservation-7",
        activation_epoch=7,
        activation_receipt="receipt-7",
        expires_at=10,
    )
    assert converted is not None
    return converted


def test_permit_atomically_converts_an_epoch_bound_held_reservation() -> None:
    checker = _checker()
    ledger = checker.VectorReservationLedger(
        capacity=(("disk", 4), ("workers", 3)),
        committed=(("disk", 0), ("workers", 0)),
    )
    held = checker.acquire_vector_reservation(
        ledger,
        reservation_id="reservation-7",
        activation_epoch=7,
        activation_receipt="receipt-7",
        demand={"workers": 3, "disk": 4},
    )
    assert held is not None
    assert (
        checker.convert_held_reservation_to_permit(
            held,
            reservation_id="reservation-7",
            activation_epoch=8,
            activation_receipt="receipt-8",
            expires_at=10,
        )
        is None
    )
    converted = checker.convert_held_reservation_to_permit(
        held,
        reservation_id="reservation-7",
        activation_epoch=7,
        activation_receipt="receipt-7",
        expires_at=10,
    )
    assert converted is not None
    charged, permit = converted
    assert charged.held == ()
    assert dict(charged.committed) == {"disk": 4, "workers": 3}
    assert permit.reservation_id == "reservation-7"
    assert permit.bounds == held.held[0].vector
    assert permit.charge_count == 1
    assert (
        checker.convert_held_reservation_to_permit(
            charged,
            reservation_id="reservation-7",
            activation_epoch=7,
            activation_receipt="receipt-7",
            expires_at=10,
        )
        is None
    )

    assert (
        checker.consume_standing_permit(
            permit,
            activation_epoch=7,
            activation_receipt="receipt-7",
            now=9,
            demand={"workers": 4},
        )
        is None
    )
    assert (
        checker.consume_standing_permit(
            permit,
            activation_epoch=7,
            activation_receipt="receipt-7",
            now=10,
            demand={"workers": 1},
        )
        is None
    )
    assert (
        checker.consume_standing_permit(
            permit,
            activation_epoch=8,
            activation_receipt="receipt-8",
            now=9,
            demand={"workers": 1},
        )
        is None
    )
    revoked = checker.revoke_standing_permit_if_authority_changed(
        permit, activation_epoch=8, activation_receipt="receipt-8"
    )
    assert revoked.revoked is True

    text = MODEL.read_text(encoding="utf-8")
    assert "ConvertHeldReservationToPermit(reservation) ==" in text
    assert "IssueStandingPermit ==" not in text
    assert "RotateActivationReceipt ==" in text
    assert "PermitConvertsHeldReservationAtomically ==" in text
    assert "PermitResourceChargeExactlyOnce ==" in text
    assert "StandingPermitDoesNotOutliveAuthority ==" in text
    assert "StandingPermitCannotExpand ==" in text


@pytest.mark.parametrize("reason", ["release", "expiry", "crash", "authority_rotation"])
def test_permit_terminal_paths_reconcile_once(reason: str) -> None:
    checker = _checker()
    charged, permit = _converted_permit(checker)
    if reason == "authority_rotation":
        permit = checker.revoke_standing_permit_if_authority_changed(
            permit, activation_epoch=8, activation_receipt="receipt-8"
        )
        assert permit.revoked
    reconciled, terminal = checker.reconcile_standing_permit(
        charged, permit, reason=reason
    )
    replayed_ledger, replayed_permit = checker.reconcile_standing_permit(
        reconciled, terminal, reason=reason
    )
    assert replayed_ledger == reconciled
    assert replayed_permit == terminal
    assert dict(reconciled.committed) == {"disk": 0, "workers": 0}
    assert reconciled.released == {"reservation-7"}
    assert terminal.reconciled
    assert terminal.terminal_reason == reason


def test_concurrent_permit_consumers_cannot_exceed_reservation() -> None:
    checker = _checker()
    _, permit = _converted_permit(checker)
    first = checker.consume_standing_permit(
        permit,
        activation_epoch=7,
        activation_receipt="receipt-7",
        now=1,
        demand={"workers": 2, "disk": 1},
    )
    assert first is not None
    second = checker.consume_standing_permit(
        first,
        activation_epoch=7,
        activation_receipt="receipt-7",
        now=1,
        demand={"workers": 2},
    )
    assert second is None
    fitting = checker.consume_standing_permit(
        first,
        activation_epoch=7,
        activation_receipt="receipt-7",
        now=1,
        demand={"workers": 1, "disk": 3},
    )
    assert fitting is not None
    assert dict(fitting.consumed) == {"disk": 4, "workers": 3}
