# Deployment restore-point safety model

## System

This model covers the restore-point and operator-authority boundary behind
`okg up`: exact target selection, independently verified database recovery,
irreversible smoke-publish provider effects, phase interruption, and resume.
It catches mutation without a verified restore point, silent target
substitution, hidden provider effects, and replay of an uncertain provider
call.

## Design Contract

- Safety property: every database mutation requires an exact target and an
  exact verified restore receipt; external provider effects remain disclosed
  as irreversible and an uncertain provider effect is never replayed.
- Authority boundary: the normalized restore receipt, confirmed safety-plan
  digest, requested/resolved target identity, and durable per-phase attempt
  record jointly authorize mutation and recovery. `--skip-backup` contributes
  no authority by itself.
- Valid runtime paths: a current verified backup, an exact independently
  verified receipt handoff, explicit target confirmation, ordered 14-phase
  execution, exact terminal-receipt reconciliation, and proven-no-effect
  supersession.
- Out of scope: provider-side rollback, concrete archive bytes, database row
  contents, process supervision, and queue/doctor health are covered by the
  disposable runtime and integration tests rather than this finite model.
- Preconditions/refinement: `tests/pact/test_deployment_restore_point_model_contract.py`
  binds the finite predicates to concrete attempt identity, receipt, target,
  restore, effect-digest, supersession, and recovery fields; execution behavior
  is checked in `tests/substrate/test_deployment_upgrade.py` and
  `tests/substrate/e2e/test_deployment_lifecycle_execution.py`.

## Model Inventory

- `DeploymentRestorePoint.tla` is the positive 14-phase restore-point,
  disclosure, interruption, observation, and resume model.
- `DeploymentRestorePoint.cfg` checks the complete positive safety invariant
  set.
- `DeploymentRestorePointCounterexample.tla` contains explicit unsafe actions
  used only by the five negative witnesses and the positive handoff
  reachability witness.
- `MissingRestoreCounterexample.cfg`, `TargetSubstitutionCounterexample.cfg`,
  `ExternalReplayCounterexample.cfg`, `SkipBackupCounterexample.cfg`, and
  `ExternalDisclosureCounterexample.cfg` must each produce the named invariant
  violation.
- `IndependentHandoffPositive.cfg` must violate
  `IndependentHandoffUnreachable`, proving that `--skip-backup` plus an exact
  independent receipt can reach mutation.

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `DeploymentRestorePoint.tla` | Admit an exact restore point, confirm the exact target/effect plan, start and finish phases, record interruption evidence, reconcile, and resume. | `src/okg/substrate/deployment_upgrade.py`, `src/okg/substrate/deployment_lifecycle.py` | `tests/pact/test_deployment_restore_point_model_contract.py`, `tests/substrate/test_deployment_upgrade.py`, `tests/substrate/test_deployment_lifecycle.py` |
| `DeploymentRestorePointCounterexample.tla` | Remove one authority or disclosure guard at a time and demonstrate the resulting unsafe path; also demonstrate the supported independent-receipt handoff. | `src/okg/substrate/deployment_upgrade.py`, `src/okg/substrate/cli/deployment.py` | `tests/pact/test_deployment_restore_point_model_contract.py`, `tests/substrate/e2e/test_deployment_lifecycle_execution.py` |

## Invariant Map

| Invariant | What it means | Code/test signal |
| --- | --- | --- |
| `TypeInvariant` | Every phase, status, receipt, effect, and authority value remains in its declared domain. | Model-contract tests and the strict formal report reject malformed state. |
| `MutationRequiresExactTargetAndVerifiedRestore` | Mutation starts only with exact target identity and an exact verified restore receipt. | Restore-absence, verified-backup, and target-drift tests refuse before mutation. |
| `TargetSubstitutionRequiresExplicitAcknowledgement` | A resolved target differing from the request cannot be used silently. | CLI/state/output tests require the exact confirmation digest. |
| `ExternalEffectNeverClaimsReversible` | Provider mutation is always disclosed as lacking rollback authority. | Safety-plan snapshot tests inspect plan, state, output, and recovery actions. |
| `UncertainExternalEffectIsNeverReplayed` | An interrupted provider call blocks for exact observation instead of automatic replay. | Resume tests preserve the uncertain attempt and require manual reconciliation. |
| `SkipBackupNeverAuthorizesMutation` | The flag can skip archive creation only when separate exact receipt authority exists. | Positive and negative skip-backup handoff tests distinguish receipt authority from the flag. |
| `UncertainPhaseCannotSucceed` | An unresolved interrupted phase cannot be reported as successful. | Phase-attempt state tests require terminal or no-effect evidence. |
| `AttemptGuaranteesRecorded` | Every started phase durably snapshots target, restore, plan, and effect guarantees. | Lifecycle state tests inspect the concrete per-attempt guarantee record. |
| `EffectUncertaintyIsRecorded` | Failed or interrupted effectful work records that its effect may have happened. | All-phase interruption/resume tests block replay until reconciliation. |
| `IndependentHandoffUnreachable` | This intentionally false witness proves the supported exact-receipt handoff reaches mutation. | The checker requires the named counterexample from `IndependentHandoffPositive.cfg`. |

## Verification Process

Run the real TLC checker and the executable refinement tests:

```bash
uv run python scripts/check_formal_deployment_restore_point.py
uv run --extra dev pytest -q tests/pact/test_deployment_restore_point_model_contract.py tests/substrate/test_deployment_upgrade.py tests/substrate/test_deployment_lifecycle.py
```

The checker writes
`.scratch/formal/deployment-restore-point/formal-report.yaml` plus one report
per named witness. It fails on a missing TLC installation, unexpected witness,
invariant violation, timeout, or vacuous state space.

## Model Boundary

The positive model includes all 14 runtime phases but deliberately collapses
historical attempt records and concrete digests into exactness predicates.
`safetyPlanExact` represents requested/resolved target plus lifecycle, adapter,
and effect digests; `restoreReceiptExact` represents normalized archive,
freshness, ownership, and restore checks; `attemptGuarantees` represents the
durable phase snapshot. Exact-observation actions abstract trusted
phase-specific reconcilers, not operator assertions. Concrete archive bytes,
provider calls, published row deltas, queue/DLQ bounds, doctor, and readiness
must remain proven by the runtime tests and disposable deployment evidence.
