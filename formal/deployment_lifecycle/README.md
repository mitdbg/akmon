# Deployment lifecycle model

## System

This model covers the phase controller behind `okg deployment up`: preflight,
backup, service quiescence, database/catalog mutation, smoke publication,
readiness, cancellation, and cleanup. It catches replay of proven expensive
work and terminal states that retain service or disposable-resource debt.

## Design Contract

- Safety property: no disruptive effect precedes preflight; a completed phase
  replays only after a dependent semantic input changes; clean terminal states
  restore the prior service state and own no disposable resources.
- Authority boundary: persisted phase receipts and cleanup obligations are the
  authority for resume, invalidation, compensation, and terminal disposition.
- Valid runtime paths: ordered success, phase failure, cancellation, uncertain
  effect observation, option-local invalidation, cleanup retry, and failed-run
  resume.
- Out of scope: database row contents, archive bytes, source FactBatch contents,
  and worker process mechanics are checked by runtime and integration tests.
- Preconditions/refinement: `tests/pact/test_deployment_lifecycle_model_contract.py`
  binds the named invariants to the executable phase implementation, while
  `tests/substrate/test_deployment_lifecycle.py` checks fingerprints, receipts,
  compensation, and the integrated evidence gate.

## Model Inventory

- `DeploymentLifecycle.tla` defines phase transitions, invalidation,
  cancellation, uncertain-effect observation, compensation, and cleanup.
- `DeploymentLifecycle.cfg` runs `Spec` and all safety invariants used by the
  registered checker.

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `DeploymentLifecycle.tla` | Start/finish a phase, persist or observe a receipt, invalidate dependents, cancel, compensate, and clean terminal resources. | `src/okg/substrate/deployment_lifecycle.py`, `src/okg/substrate/deployment_upgrade.py` | `tests/pact/test_deployment_lifecycle_model_contract.py`, `tests/substrate/test_deployment_lifecycle.py`, `tests/substrate/test_deployment_upgrade.py` |

## Invariant Map

| Invariant | What it means | Code/test signal |
| --- | --- | --- |
| `TypeInvariant` | Every state value remains in its declared phase, resource, receipt, and outcome domain. | Model contract and TLC report-producer tests reject malformed state. |
| `NoDisruptiveEffectBeforePreflight` | Backup, service, mutation, and publication effects require completed preflight. | Ordered-phase tests assert preflight is first. |
| `NoMutationBeforePreflight` | Migration and catalog mutation cannot run before preflight. | Failure-boundary tests assert no later phase runs after preflight failure. |
| `CompletedPhaseReplayRequiresInvalidation` | Proven work is not replayed without a semantic dependency change. | Receipt-resume tests compare phase call counts. |
| `InputChangeInvalidatesOnlyDependents` | Option drift invalidates only its explicit dependency closure. | Fingerprint tests vary smoke, readiness, revision, manifest, and backup inputs. |
| `CleanTerminalRestoresPriorServiceState` | A clean outcome preserves the service state observed before the attempt. | Compensation tests inspect the persisted service obligation and cleanup result. |
| `CleanTerminalOwnsNoDisposableResources` | Clean outcomes retain no child, verification database, temporary archive, or cleanup debt. | Boundary-failure tests assert the durable obligation set is empty. |
| `UnknownEffectCannotBeClean` | An uncertain phase cannot be reported as a clean terminal result. | Receipt reconciliation blocks unmatched active attempts. |
| `CleanupFailureCannotBeClean` | Failed compensation remains blocked with evidence. | Cleanup failure tests require a recovery command and non-clean status. |
| `CancellationRequiresCompensation` | Cancellation enters recovery and cannot bypass cleanup. | Cancellation/refinement tests assert compensation remains required. |
| `UncertainCompletionRequiresReceipt` | An uncertain phase becomes complete only after a matching observation receipt. | Formal and receipt-reconciliation tests reject inferred completion. |

## Verification Process

Run the registered checker and executable refinement suite:

```bash
uv run python scripts/check_formal_deployment_lifecycle.py
uv run --extra dev pytest -q tests/pact/test_deployment_lifecycle_model_contract.py tests/substrate/test_deployment_lifecycle.py
```

The checker writes `.scratch/formal/deployment-lifecycle/formal-report.yaml` and
fails on a missing invariant, invariant violation, timeout, or vacuous state
space.

## Model Boundary

The model abstracts concrete database identities, archive validation, process
groups, source payloads, and storage reclamation measurements. Those must stay
covered by `tests/substrate/test_deployment_upgrade.py` and disposable
deployment qualification; a model pass alone does not authorize destructive
work or establish final deployment readiness.
