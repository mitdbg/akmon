# Fleet Capacity Reservation

## System

This bounded TLA+ model describes admission against one deduplicated shared
resource domain, held and consumed reservations, crash reconciliation, and an
activation-bound standing permit converted from a previously held vector. It
targets concurrent local OKG operations that otherwise double-count shared
capacity, oversubscribe the host, leak a crashed reservation, or reuse a permit
after its activation authority changes.

## Design Contract

- Safety property: locked admission never exceeds the declared capacity vector;
  each reservation charge and terminal release happens at most once; and a
  standing permit cannot expand, outlive its activation receipt, or be consumed
  beyond its converted reservation.
- Authority boundary: the intended fleet-capacity controller owns reservation
  acquisition, atomic held-reservation conversion, and terminal reconciliation.
  Existing headroom probes and upgrade preflights are measurement/refinement
  surfaces, not evidence that the fleet reservation controller has shipped.
- Valid runtime paths: acquire and consume a fitting reservation, refuse an
  oversubscribing reservation, reconcile a crashed hold, convert one exact
  epoch-bound vector to a permit, bounded permit consumption, release, expiry,
  crash, authority rotation, and replay-safe terminal reconciliation.
- Out of scope: PostgreSQL lock implementation, host and container discovery,
  measurement freshness, process scheduling, distributed arbitration, service
  rollout, and live fleet availability remain runtime and deployment proof
  obligations.
- Preconditions/refinement:
  `tests/substrate/test_local_postgres_fleet_capacity.py` exercises the
  executable reservation and permit helpers and binds each modeled invariant;
  the current host probe in `src/okg/substrate/runtime/host_headroom.py` must be
  integrated by the future controller before this becomes product behavior.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `FleetCapacityReservation.tla` | `FleetCapacityReservation.cfg` | Bounded scalar/vector capacity, shared-domain deduplication, crash reconciliation, standing-permit conversion, consumption, authority rotation, and terminal release. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `FleetCapacityReservation.tla` | Target transformation from measured host/database resource domains to locked held and committed reservation vectors without double-counting shared capacity. | `src/okg/substrate/runtime/host_headroom.py`, `src/okg/substrate/deployment_upgrade.py` | `tests/substrate/test_local_postgres_fleet_capacity.py::test_concurrent_reservations_never_oversubscribe`, `tests/substrate/test_local_postgres_fleet_capacity.py::test_resource_domains_deduplicate_shared_capacity` |
| `FleetCapacityReservation.tla` | Target transformation that atomically converts one activation-bound held vector to a bounded standing permit, then releases its charge once on every terminal path. | `src/okg/substrate/deployment_authority.py`, `src/okg/substrate/deployment_receipt.py` | `tests/substrate/test_local_postgres_fleet_capacity.py::test_permit_atomically_converts_an_epoch_bound_held_reservation`, `tests/substrate/test_local_postgres_fleet_capacity.py::test_permit_terminal_paths_reconcile_once`, `tests/substrate/test_local_postgres_fleet_capacity.py::test_concurrent_permit_consumers_cannot_exceed_reservation` |

These are implementation targets and current measurement/authority surfaces;
the map does not claim that they already contain the modeled reservation
ledger or standing-permit protocol.

## Invariant Map

| Invariant | What it means | Code/test signal |
| --- | --- | --- |
| `ReservationLifecycleTyped` | Reservation, crash, reconciliation, binding, charge, and committed-demand state remains in its finite domain. | Model and helper checks in `tests/substrate/test_local_postgres_fleet_capacity.py`. |
| `ConcurrentReservationsNeverOversubscribe` | Observed, standing, committed, and held demand never exceeds capacity. | `tests/substrate/test_local_postgres_fleet_capacity.py::test_concurrent_reservations_never_oversubscribe`. |
| `CrashedReservationReconcilesAtMostOnce` | A crashed hold is released once and replay is an exact no-op. | `tests/substrate/test_local_postgres_fleet_capacity.py::test_crashed_reservation_reconciles_once`. |
| `ConsumedReservationsRemainCommitted` | Committed demand exactly matches active consumed reservations. | Reservation and permit conversion tests in `tests/substrate/test_local_postgres_fleet_capacity.py`. |
| `ResourceDomainsDeduplicateSharedCapacity` | Deployments sharing one resource domain contribute one observed-domain demand. | `tests/substrate/test_local_postgres_fleet_capacity.py::test_resource_domains_deduplicate_shared_capacity`. |
| `StandingPermitLifecycleTyped` | Permit state, binding, bounds, charge, reconciliation, and time fields stay in their finite domain. | Permit lifecycle helpers in `tests/substrate/test_local_postgres_fleet_capacity.py`. |
| `StandingPermitConsumptionBounded` | Permit consumption never exceeds either resource bound. | `tests/substrate/test_local_postgres_fleet_capacity.py::test_concurrent_permit_consumers_cannot_exceed_reservation`. |
| `StandingPermitDoesNotOutliveAuthority` | An issued permit matches the current activation epoch and receipt and is unexpired. | Authority-rotation and expiry cases in `tests/substrate/test_local_postgres_fleet_capacity.py::test_permit_atomically_converts_an_epoch_bound_held_reservation`. |
| `StandingPermitCannotExpand` | A permit's resource vector and authority binding equal the reservation from which it was converted. | Atomic conversion checks in `tests/substrate/test_local_postgres_fleet_capacity.py`. |
| `PermitConvertsHeldReservationAtomically` | Conversion consumes and charges exactly the selected held reservation. | `tests/substrate/test_local_postgres_fleet_capacity.py::test_permit_atomically_converts_an_epoch_bound_held_reservation`. |
| `PermitResourceChargeExactlyOnce` | The permit charge count equals the set of charged reservations. | Conversion replay refusal in `tests/substrate/test_local_postgres_fleet_capacity.py`. |
| `PermitTerminalReconciliationAtMostOnce` | Released and reconciled permits return capacity once; unfinished terminal states remain unreconciled. | `tests/substrate/test_local_postgres_fleet_capacity.py::test_permit_terminal_paths_reconcile_once`. |

## Verification Process

Run the complete bounded model with:

```sh
uv run --extra dev python scripts/check_formal_fleet_capacity_reservation.py \
  --all --out .scratch/formal/fleet-capacity-reservation
```

The checker runs `FleetCapacityReservation.tla` with
`FleetCapacityReservation.cfg`, enforces a non-vacuity floor, and emits
`formal-report.yaml`. It is registered as a full-model checker in
`scripts/run_formal_suite.py`, so the per-PR formal smoke suite executes this
same complete bounded configuration.

Run the implementation-refinement helpers with:

```sh
uv run --extra dev pytest tests/substrate/test_local_postgres_fleet_capacity.py
```

The report is bounded model evidence. It is not evidence of real reservations,
host headroom, active locks, upgraded services, or a healthy local fleet.

## Model Boundary

The model uses two consumers, two reservation IDs, one deduplicated domain, a
small scalar capacity, and a two-resource permit vector. It treats acquisition,
conversion, and reconciliation as atomic and assumes capacity observations are
authentic. It does not prove measurement freshness, database serialization,
cross-process recovery, or deployment availability; those require the future
controller refinement, runtime tests, and live operational evidence.
