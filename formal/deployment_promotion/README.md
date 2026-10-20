# Deployment Promotion Formal Model

## System

`DeploymentPromotion.tla` models the serialized transition of an artifact that
passed staging into production. It makes approval, lock ownership, writer
fencing, queue drain, backup verification, six production mutation boundaries,
successful readiness, rollback, and blocked recovery explicit for two competing
promotion identities.

## Design Contract

- Safety property: Production mutation occurs only for the approved artifact,
  under one promotion owner, after writers are fenced and a post-fence backup is
  verified; writers reopen only in the completed candidate state or after the
  previous database, artifact, and readiness are restored.
- Authority boundary: `src/okg/substrate/deployment_promotion.py` owns durable
  transition ordering, while `src/okg/substrate/runtime/promotion_fence.py` owns
  PostgreSQL-authoritative lock, writer, and schedule exclusion.
- Valid runtime paths: staging rejection without production mutation, successful
  candidate activation through every mutation boundary, pre-mutation abort, and
  recovery to either the previous ready state or an explicit blocked state with
  writers still fenced.
- Out of scope: Filesystem durability, PostgreSQL command semantics, DBOS queue
  implementation, service-manager behavior, receipt truthfulness, and endpoint
  probes remain runtime-test and live deployment evidence obligations.
- Preconditions/refinement: Runtime tests in
  `tests/substrate/test_deployment_promotion.py`,
  `tests/substrate/test_deployment_promotion_operations.py`, and
  `tests/substrate/runtime/test_promotion_fence.py` exercise the concrete
  boundaries; `tests/pact/test_deployment_promotion_model_contract.py` binds the
  positive model and retained witnesses to the PACT evidence contract.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `DeploymentPromotion.tla` | `DeploymentPromotion.cfg` | Positive two-contender model covering approval, fencing, verified backup, six mutations, readiness, rollback, blocked recovery, and recovery liveness. |
| `DeploymentPromotionCounterexample.tla` | `DeploymentPromotionBackupGuardWitness.cfg`, `DeploymentPromotionLockGuardWitness.cfg`, `DeploymentPromotionWriterReadyGuardWitness.cfg`, `DeploymentPromotionRestoreOrderGuardWitness.cfg` | Retained negative witnesses that remove one backup, ownership, readiness, or restore-order guard and must violate exactly the named production invariant. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `DeploymentPromotion.tla` | One approved promotion acquires production authority, fences writers and schedules, verifies a post-fence backup, applies ordered mutations, proves readiness, and either completes or recovers fail-closed. | `src/okg/substrate/deployment_promotion.py`, `src/okg/substrate/deployment_promotion_operations.py`, `src/okg/substrate/runtime/promotion_fence.py` | `tests/substrate/test_deployment_promotion.py`, `tests/substrate/test_deployment_promotion_operations.py`, `tests/substrate/runtime/test_promotion_fence.py` |
| `DeploymentPromotionCounterexample.tla` | Each witness removes one critical transition guard so TLC proves that backup, lock ownership, writer readiness, and restore ordering are load-bearing. | `src/okg/substrate/deployment_promotion.py`, `src/okg/substrate/deployment_promotion_operations.py`, `src/okg/substrate/runtime/promotion_fence.py` | `tests/pact/test_deployment_promotion_model_contract.py`, `tests/pact/test_formal_report_producers.py` |

## Invariant Map

| Invariant or property | What it means | Code/test signal |
| --- | --- | --- |
| `TypeInvariant` | Promotion identities, phases, artifacts, database versions, mutation boundaries, lock owners, and recovery failures stay in the finite modeled domain. | Strict model/report checks in `tests/pact/test_deployment_promotion_model_contract.py` and `tests/pact/test_formal_report_producers.py`. |
| `NoProductionMutationBeforeStagingReady` | Failed or unapproved staging cannot change production state. | Staging-isolation tests in `tests/substrate/test_deployment_promotion.py`. |
| `OneActivePromotion` | At most one promotion owns production mutation authority. | PostgreSQL advisory-lock and contender tests in `tests/substrate/runtime/test_promotion_fence.py`; `DeploymentPromotionLockGuardWitness.cfg` must violate this invariant. |
| `MutationRequiresVerifiedPostFenceBackup` | No production mutation occurs before a backup created after fencing has restored and passed verification. | Backup identity and invalid-backup tests in `tests/substrate/test_deployment_promotion.py` and `tests/substrate/test_deployment_promotion_operations.py`; `DeploymentPromotionBackupGuardWitness.cfg` must violate this invariant. |
| `MutationBoundaryRequiresLockAndFence` | Every release, database, policy, service, publish, and readiness mutation retains lock and writer-fence authority. | Boundary fault injection in `tests/substrate/test_deployment_promotion.py`. |
| `WritersOpenOnlyInStableState` | Writers are open only before mutation, after successful candidate completion, or after proven rollback. | Fence acquire/release and blocked-recovery tests in `tests/substrate/runtime/test_promotion_fence.py` and `tests/substrate/test_deployment_promotion.py`. |
| `CompletedUsesApprovedArtifact` | Successful production readiness and completion identify the exact approved sealed artifact. | Artifact and descriptor binding tests in `tests/substrate/test_deployment_promotion.py`. |
| `RollbackReopensOnlyPreviousState` | Rollback cannot reopen writers until the previous database, artifact, and readiness are all restored. | Rollback boundary tests in `tests/substrate/test_deployment_promotion.py`; `DeploymentPromotionWriterReadyGuardWitness.cfg` must violate this invariant. |
| `PreviousArtifactRequiresRestoredDatabase` | The previous release cannot be reactivated over candidate database state. | Exact database recreation and restore-order tests in `tests/substrate/test_deployment_promotion_operations.py`; `DeploymentPromotionRestoreOrderGuardWitness.cfg` must violate this invariant. |
| `InvalidBackupNeverMutates` | A backup that cannot be verified prevents every production mutation. | Invalid-backup fault test in `tests/substrate/test_deployment_promotion.py`. |
| `InvalidBackupRetainsFenceUntilReady` | Backup-gate abort keeps writers fenced until unchanged previous-version readiness is proven. | Pre-mutation abort and readiness tests in `tests/substrate/test_deployment_promotion.py`. |
| `RecoveryBlockedKeepsWritersFenced` | A restore, reactivation, or readiness failure becomes an explicit blocker and never silently reopens writers. | Blocked-recovery tests in `tests/substrate/test_deployment_promotion.py`. |
| `ProductAdmissionNeverMutates` | Admitting a first-install product request records identity only: it does not acquire the production lock, fence writers, drain queues, create or verify a backup, mutate production, or change the active artifact or database. | Admission-only journal and no-effect assertions in `tests/substrate/test_deployment_promotion.py` exercise `src/okg/substrate/deployment_product_lifecycle.py` and `src/okg/substrate/deployment_promotion.py`. |
| `RecoveryConverges` | Under weak fairness, every recovery path eventually completes rollback or reaches an injected blocked-recovery state instead of stuttering forever. | Positive TLC run through `scripts/check_formal_deployment_promotion.py`. |
| `RecoveryWithoutInjectedFailureSucceeds` | When no recovery failure is injected, rollback eventually restores the previous ready state. | Positive TLC liveness proof and model-contract tests in `tests/pact/test_deployment_promotion_model_contract.py`. |

## Verification Process

Run the positive proof with:

```sh
uv run --extra dev python scripts/check_formal_deployment_promotion.py \
  --out .scratch/formal/deployment-promotion
```

The positive config checks every safety invariant plus `RecoveryConverges` and
`RecoveryWithoutInjectedFailureSucceeds`. Weak fairness on the recovery
transition prevents indefinite recovery stutter. A named injected restore,
reactivation, or readiness failure reaches a blocking state; without an
injected failure, rollback must complete.

Run all retained witnesses with:

```sh
uv run --extra dev python scripts/run_formal_suite.py --mode counterexamples
```

| Witness | Removed guard | Expected violation |
| --- | --- | --- |
| `DeploymentPromotionBackupGuardWitness.cfg` | verified backup before release activation | `MutationRequiresVerifiedPostFenceBackup` |
| `DeploymentPromotionLockGuardWitness.cfg` | single-owner acquisition while another promotion owns the lock | `OneActivePromotion` |
| `DeploymentPromotionWriterReadyGuardWitness.cfg` | previous-version readiness before reopening writers | `RollbackReopensOnlyPreviousState` |
| `DeploymentPromotionRestoreOrderGuardWitness.cfg` | database restore before old release activation | `PreviousArtifactRequiresRestoredDatabase` |

## Model Boundary

The model treats each controller transition as atomic and assumes operation
receipts truthfully represent durable effects. It does not prove external
command behavior, PostgreSQL backup fidelity, real DBOS drain semantics,
service-manager execution, filesystem crash consistency, or endpoint ownership.
Those remain obligations of the production-bound tests named in the
Transformation Map and of live promotion readiness evidence.
