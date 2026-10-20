# DBOS Epoch Supersession

## System

This bounded model covers DBOS workflow identity when a local OKG deployment
activates a new application, lifecycle epoch, receipt, database-writer epoch,
worktree capability epoch, and branch binding. It targets stale queued work,
running bodies, leases, database transactions, and recovery claims that survive
an activation boundary and then attempt to mutate current state or publish an
outcome.

## Design Contract

- Safety property: only work carrying the complete current binding may mutate
  or publish; superseded bodies, producers, leases, and transactions are fenced,
  and at most one current recovery owns an operation equivalence class.
- Authority boundary: the deployment activation controller owns exclusive epoch
  transitions, while admitted DBOS bodies hold the shared mutation fence through
  commit or refusal. The boundary registry names the required identity fields
  and production entry points; the model does not create authority for them.
- Valid runtime paths: enqueue and dequeue with the complete binding, current
  body mutation and outcome publication, exclusive activation after shared
  holders drain, refusal of stale mutation or transaction commit, supersession,
  quarantine, and one current recovery selection.
- Out of scope: DBOS scheduler internals, PostgreSQL advisory-lock fairness,
  process crashes, network delivery, real transaction isolation, deployment
  rollout, graph publication, and live service availability remain runtime and
  deployment verification responsibilities.
- Preconditions/refinement: the static boundary inventory must match the
  production source tree, and
  `tests/substrate/test_local_fleet_workflow_supersession.py` must exercise the
  concrete binding, recovery, transaction, inventory, and shared/exclusive
  fence behavior. Passing this model alone is not product or live-deployment
  proof.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `DbosEpochSupersession.tla` | `DbosEpochSupersession.cfg` | Bounded current-versus-superseded workflow, mutation, recovery, outcome, and transaction state machine. |
| `boundary-registry.yaml` | `boundary-inventory.yaml` | Static identity-field, fence-mode, producer, and mutation-boundary refinement inventory validated before TLC runs. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `DbosEpochSupersession.tla` | Carry the complete activation binding through enqueue, body entry, source-sync mutation, and publisher mutation/publication boundaries; refuse stale work. | `src/okg/substrate/runtime/dbos_enqueue.py`, `src/okg/substrate/runtime/source_sync_workflow.py`, `src/okg/substrate/runtime/publisher_workflow.py` | `tests/substrate/test_local_fleet_workflow_supersession.py::test_epoch_binding_registry_is_complete`, `tests/substrate/test_local_fleet_workflow_supersession.py::test_superseded_body_cannot_mutate` |
| `DbosEpochSupersession.tla` | Hold shared mutation fences for admitted work, drain them before exclusive activation, reject a transaction whose epoch or receipt is stale, and select one current recovery per equivalence class. | `src/okg/substrate/runtime/promotion_fence.py`, `src/okg/substrate/runtime/worker.py` | `tests/substrate/test_local_fleet_workflow_supersession.py::test_shared_and_exclusive_epoch_fence_is_real`, `tests/substrate/test_local_fleet_workflow_supersession.py::test_postgres_transaction_cannot_commit_across_epoch_advance`, `tests/substrate/test_local_fleet_workflow_supersession.py::test_single_current_recovery_per_equivalence_class` |

## Invariant Map

| Invariant | What it means | Code/test signal |
| --- | --- | --- |
| `TypeInvariant` | Every modeled binding, body, producer, lease, recovery, outcome, and transaction value remains in its declared finite domain. | Model-contract and strict-report checks in `tests/substrate/test_local_fleet_workflow_supersession.py`. |
| `EpochBindingComplete` | Application, lifecycle epoch, and activation receipt identify the current binding together; a partial current identity is invalid. | `tests/substrate/test_local_fleet_workflow_supersession.py::test_epoch_binding_registry_is_complete`. |
| `SupersededBodyCannotMutate` | Superseded work is fenced and cannot retain a mutation. | `tests/substrate/test_local_fleet_workflow_supersession.py::test_superseded_body_cannot_mutate`. |
| `StaleBindingNeverMutates` | Any non-current binding has zero admitted mutations. | `tests/substrate/test_local_fleet_workflow_supersession.py::test_superseded_body_cannot_mutate`. |
| `SingleCurrentRecovery` | At most one recovery exists, and its owner is the current application. | `tests/substrate/test_local_fleet_workflow_supersession.py::test_single_current_recovery_per_equivalence_class`. |
| `LeaseFencedOnSupersession` | Superseded work retains no lease owner. | Boundary-registry and superseded-body checks in `tests/substrate/test_local_fleet_workflow_supersession.py`. |
| `SupersededProducerCannotRecreate` | A superseded producer cannot recreate runnable work. | Producer inventory coverage in `tests/substrate/test_local_fleet_workflow_supersession.py::test_direct_producer_registry_is_exhaustive`. |
| `OutcomeOwnedByCurrentEpoch` | Only the current binding publishes; stale outcomes are quarantined. | Current-binding and recovery selection tests in `tests/substrate/test_local_fleet_workflow_supersession.py`. |
| `NoStaleTransactionCommitAcrossEpochAdvance` | A database transaction cannot commit after its epoch or receipt ceases to match current authority. | `tests/substrate/test_local_fleet_workflow_supersession.py::test_postgres_transaction_cannot_commit_across_epoch_advance`. |
| `SharedAndExclusiveFencesNeverOverlap` | Activation's exclusive fence and a workflow's shared mutation fence cannot be held together. | `tests/substrate/test_local_fleet_workflow_supersession.py::test_shared_and_exclusive_epoch_fence_is_real`. |
| `MutationBoundaryCarriesCompleteIdentity` | Every registered mutation boundary declares the complete wire-field profile. | `tests/substrate/test_local_fleet_workflow_supersession.py::test_epoch_binding_registry_is_complete` and inventory validation. |
| `RecoveryIsUniquePerEquivalenceClass` | Recovery selects one exact-current operation for each deployment, operation kind, and logical target. | `tests/substrate/test_local_fleet_workflow_supersession.py::test_single_current_recovery_per_equivalence_class`. |

## Verification Process

Run the complete bounded model and registry validation with:

```sh
uv run --extra dev python scripts/check_formal_dbos_epoch_supersession.py \
  --all --out .scratch/formal/dbos-epoch-supersession
```

The checker validates `boundary-registry.yaml` against
`boundary-inventory.yaml` and the live source tree, runs
`DbosEpochSupersession.tla` with `DbosEpochSupersession.cfg`, enforces a
non-vacuity floor, and emits `formal-report.yaml`. The aggregate formal suite
also invokes the same full bounded model through
`scripts/run_formal_suite.py --mode smoke`.

Boundary categories are derived from code alone. The scan reads each function's
syntax tree for identifiers — names, attributes, arguments, keywords, import
paths, return annotations — and for string literals that read as SQL
statements. Comments never reach the tree, and docstrings, bare-word literals
and English sentences are dropped as prose. Vocabulary used in prose therefore
neither enrols nor un-enrols a boundary. Decorators are read separately, for
the `dbos_decorated_entry` category only, and contribute no vocabulary.

Because the scan is deliberately narrow, it misses real boundaries whose
mutation happens one call deep in a helper, or whose subsystem is named only in
prose. Such an entry carries `explicit: true` and a `reason` saying why the scan
cannot see it. An explicit entry is an assertion rather than a measurement: it
stands in place of discovery for that function, must name a function that
exists, and must give a non-empty reason. Adding one is a deliberate act, which
is the point — a comment can no longer make or break a formal claim.

Run the implementation refinement tests with:

```sh
uv run --extra dev pytest tests/substrate/test_local_fleet_workflow_supersession.py
```

The strict report is eligible for PACT ingestion only as bounded formal and
static-refinement evidence. It does not substitute for a deployed upgrade,
published generation, queue/DLQ check, doctor result, or live graph read.

## Model Boundary

The model collapses many deployments and operations to one current and one
stale binding, treats registry discovery and lock operations as atomic, and
assumes the declared current binding is authentic. It does not prove that all
production producers already serialize the fields, that advisory locks are
held across real process and database failure boundaries, or that a local OKG
upgrade succeeds. Those remain obligations of the implementation tests named
above and of live deployment evidence.
