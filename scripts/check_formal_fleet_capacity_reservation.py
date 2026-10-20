#!/usr/bin/env python3
"""Check fleet capacity reservations and emit strict PACT formal evidence."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Iterable, Mapping

from okg.substrate.formal_tlc import TLCProperty, TLCSetupError, run_tlc_formal_report


ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = ROOT / "formal" / "fleet_capacity_reservation"
DEFAULT_OUT = ROOT / ".scratch" / "formal" / "fleet-capacity-reservation"
INVARIANTS = (
    "ReservationLifecycleTyped",
    "ConcurrentReservationsNeverOversubscribe",
    "CrashedReservationReconcilesAtMostOnce",
    "ConsumedReservationsRemainCommitted",
    "ResourceDomainsDeduplicateSharedCapacity",
    "StandingPermitLifecycleTyped",
    "StandingPermitConsumptionBounded",
    "StandingPermitDoesNotOutliveAuthority",
    "StandingPermitCannotExpand",
    "PermitConvertsHeldReservationAtomically",
    "PermitResourceChargeExactlyOnce",
    "PermitTerminalReconciliationAtMostOnce",
)
REFINEMENT_TESTS = (
    "tests/substrate/test_local_postgres_fleet_capacity.py::test_concurrent_reservations_never_oversubscribe",
    "tests/substrate/test_local_postgres_fleet_capacity.py::test_crashed_reservation_reconciles_once",
    "tests/substrate/test_local_postgres_fleet_capacity.py::test_resource_domains_deduplicate_shared_capacity",
    "tests/substrate/test_local_postgres_fleet_capacity.py::test_permit_atomically_converts_an_epoch_bound_held_reservation",
    "tests/substrate/test_local_postgres_fleet_capacity.py::test_permit_terminal_paths_reconcile_once",
    "tests/substrate/test_local_postgres_fleet_capacity.py::test_concurrent_permit_consumers_cannot_exceed_reservation",
)
PROPERTY = TLCProperty(
    "FleetCapacityReservationAggregate",
    (
        "TLC verified locked admission never oversubscribes a resource domain, "
        "crash reconciliation is at-most-once, standing permits atomically "
        "convert already-held epoch-bound vector reservations with exactly one "
        "charge, and shared observations are deduplicated by domain identity."
    ),
    refinement_tests=REFINEMENT_TESTS,
    expected_invariants=INVARIANTS,
)


@dataclass(frozen=True)
class ReservationState:
    capacity: int
    standing_reserve: int
    committed: int = 0
    held: tuple[tuple[str, int], ...] = ()
    reconciled: frozenset[str] = frozenset()

    @property
    def total(self) -> int:
        return (
            self.standing_reserve
            + self.committed
            + sum(demand for _, demand in self.held)
        )


@dataclass(frozen=True)
class StandingPermit:
    reservation_id: str
    activation_epoch: int
    activation_receipt: str
    expires_at: int
    bounds: tuple[tuple[str, int], ...]
    consumed: tuple[tuple[str, int], ...]
    revoked: bool = False
    reconciled: bool = False
    terminal_reason: str | None = None
    charge_count: int = 1


@dataclass(frozen=True)
class HeldVectorReservation:
    reservation_id: str
    activation_epoch: int
    activation_receipt: str
    vector: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class VectorReservationLedger:
    capacity: tuple[tuple[str, int], ...]
    committed: tuple[tuple[str, int], ...]
    held: tuple[HeldVectorReservation, ...] = ()
    released: frozenset[str] = frozenset()


def _vector(items: tuple[tuple[str, int], ...]) -> dict[str, int]:
    return dict(items)


def acquire_vector_reservation(
    ledger: VectorReservationLedger,
    *,
    reservation_id: str,
    activation_epoch: int,
    activation_receipt: str,
    demand: Mapping[str, int],
) -> VectorReservationLedger | None:
    """Acquire an epoch-bound vector reservation before permit conversion."""
    if (
        not reservation_id
        or reservation_id in ledger.released
        or any(item.reservation_id == reservation_id for item in ledger.held)
        or activation_epoch <= 0
        or not activation_receipt
        or not demand
        or any(int(value) <= 0 for value in demand.values())
    ):
        return None
    capacity = _vector(ledger.capacity)
    committed = _vector(ledger.committed)
    held_total = {resource: 0 for resource in capacity}
    for reservation in ledger.held:
        for resource, amount in reservation.vector:
            held_total[resource] = held_total.get(resource, 0) + amount
    vector = tuple(sorted((str(key), int(value)) for key, value in demand.items()))
    for resource, amount in vector:
        if resource not in capacity:
            return None
        if (
            committed.get(resource, 0) + held_total.get(resource, 0) + amount
            > capacity[resource]
        ):
            return None
    return replace(
        ledger,
        held=(
            *ledger.held,
            HeldVectorReservation(
                reservation_id=reservation_id,
                activation_epoch=activation_epoch,
                activation_receipt=activation_receipt,
                vector=vector,
            ),
        ),
    )


def convert_held_reservation_to_permit(
    ledger: VectorReservationLedger,
    *,
    reservation_id: str,
    activation_epoch: int,
    activation_receipt: str,
    expires_at: int,
) -> tuple[VectorReservationLedger, StandingPermit] | None:
    """Atomically convert one already-held vector into one charged permit."""
    reservation = next(
        (item for item in ledger.held if item.reservation_id == reservation_id), None
    )
    if (
        reservation is None
        or reservation.activation_epoch != activation_epoch
        or reservation.activation_receipt != activation_receipt
        or expires_at <= 0
    ):
        return None
    committed = _vector(ledger.committed)
    for resource, amount in reservation.vector:
        committed[resource] = committed.get(resource, 0) + amount
    updated = replace(
        ledger,
        held=tuple(item for item in ledger.held if item != reservation),
        committed=tuple(sorted(committed.items())),
    )
    permit = StandingPermit(
        reservation_id=reservation.reservation_id,
        activation_epoch=activation_epoch,
        activation_receipt=activation_receipt,
        expires_at=expires_at,
        bounds=reservation.vector,
        consumed=tuple((key, 0) for key, _ in reservation.vector),
    )
    return updated, permit


def revoke_standing_permit_if_authority_changed(
    permit: StandingPermit, *, activation_epoch: int, activation_receipt: str
) -> StandingPermit:
    if (
        permit.activation_epoch == activation_epoch
        and permit.activation_receipt == activation_receipt
    ):
        return permit
    return replace(permit, revoked=True)


def reconcile_standing_permit(
    ledger: VectorReservationLedger,
    permit: StandingPermit,
    *,
    reason: str,
) -> tuple[VectorReservationLedger, StandingPermit]:
    """Release the converted vector exactly once after release/expiry/crash/revocation."""
    if reason not in {"release", "expiry", "crash", "authority_rotation"}:
        raise ValueError("unsupported permit terminal reason")
    if permit.reconciled:
        return ledger, permit
    committed = _vector(ledger.committed)
    for resource, amount in permit.bounds:
        if committed.get(resource, 0) < amount:
            raise ValueError("permit charge is absent or already released")
        committed[resource] -= amount
    return (
        replace(
            ledger,
            committed=tuple(sorted(committed.items())),
            released=ledger.released | {permit.reservation_id},
        ),
        replace(
            permit,
            revoked=permit.revoked or reason == "authority_rotation",
            reconciled=True,
            terminal_reason=reason,
        ),
    )


def consume_standing_permit(
    permit: StandingPermit,
    *,
    activation_epoch: int,
    activation_receipt: str,
    now: int,
    demand: Mapping[str, int],
) -> StandingPermit | None:
    """Consume a bounded vector under the exact activation receipt."""
    permit = revoke_standing_permit_if_authority_changed(
        permit,
        activation_epoch=activation_epoch,
        activation_receipt=activation_receipt,
    )
    if permit.revoked or now >= permit.expires_at:
        return None
    bounds = _vector(permit.bounds)
    consumed = _vector(permit.consumed)
    if not demand or any(int(value) <= 0 for value in demand.values()):
        return None
    for resource, amount in demand.items():
        if (
            resource not in bounds
            or consumed[resource] + int(amount) > bounds[resource]
        ):
            return None
    for resource, amount in demand.items():
        consumed[resource] += int(amount)
    return replace(permit, consumed=tuple(sorted(consumed.items())))


def acquire(
    state: ReservationState, reservation_id: str, demand: int
) -> ReservationState | None:
    """CAS-shaped admission: return no state when the locked budget does not fit."""
    if demand <= 0 or reservation_id in state.reconciled:
        return None
    if any(item == reservation_id for item, _ in state.held):
        return None
    if state.total + demand > state.capacity:
        return None
    return replace(state, held=(*state.held, (reservation_id, demand)))


def reconcile_crash(state: ReservationState, reservation_id: str) -> ReservationState:
    """Release an unconsumed reservation once; replays are exact no-ops."""
    if reservation_id in state.reconciled:
        return state
    retained = tuple(item for item in state.held if item[0] != reservation_id)
    if len(retained) == len(state.held):
        return state
    return replace(
        state,
        held=retained,
        reconciled=state.reconciled | {reservation_id},
    )


def deduplicate_resource_domains(
    observations: Iterable[Mapping[str, object]],
) -> dict[str, Mapping[str, object]]:
    """Collapse duplicate capacity observations and reject conflicting facts."""
    domains: dict[str, Mapping[str, object]] = {}
    for observation in observations:
        domain = str(observation["domain_id"])
        canonical = {
            "capacity": int(observation["capacity"]),
            "observed_at": str(observation["observed_at"]),
        }
        previous = domains.get(domain)
        if previous is not None and previous != canonical:
            raise ValueError(f"conflicting observations for resource domain {domain}")
        domains[domain] = canonical
    return domains


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--all", action="store_true", help="run the complete bounded model"
    )
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    args = parser.parse_args(argv)
    command = (
        "uv run python scripts/check_formal_fleet_capacity_reservation.py --all "
        f"--out {args.out}"
    )
    try:
        result = run_tlc_formal_report(
            repo_root=ROOT,
            model_dir=MODEL_DIR,
            model=MODEL_DIR / "FleetCapacityReservation.tla",
            config=MODEL_DIR / "FleetCapacityReservation.cfg",
            out_dir=args.out,
            properties=(PROPERTY,),
            expected_properties=(PROPERTY,),
            checker_command=command,
            timeout_seconds=args.timeout_seconds,
            min_distinct_states=12,
        )
    except TLCSetupError as exc:
        print(f"formal-fleet-capacity-reservation: {exc}", file=sys.stderr)
        return exc.exit_code
    if result.run_failed:
        print(
            f"formal-fleet-capacity-reservation: failed; see {result.stdout_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(f"formal-fleet-capacity-reservation: passed; report at {result.report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
