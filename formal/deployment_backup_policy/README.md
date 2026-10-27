# Deployment Backup Policy Formal Model

## System

`DeploymentBackupPolicy.tla` models how a deployment refresh decides what
rollback proof it must hold before it is allowed to mutate the database.
A run first classifies its target plan: unknown, unreadable, or untrusted
plans refuse outright. A routine run — dev channel, dogfood readiness and
deployment class, risk-admitted policy, a closed effect inventory, and fresh
independent disaster-recovery proof — is admitted with **no physical backup
at all**. Any known physical effect (pending migrations, extension work,
catalog or Nomos change), any smoke publish that reaches an external service,
any destructive confirmation, or any production posture instead forces the
forced path: create a current archive, verify it with a full disposable
restore, and only then mutate. Target-plan drift observed after admission
revokes the admission before the mutation boundary.

External reach is deliberately a **forced-backup** classification, not a
refusal. A deployment whose smoke publish calls an LLM provider cannot undo
those calls, but it can still undo its database, and that restore point is
the guarantee the backup phase exists to provide. Classifying it as a refusal
left such a deployment with no supported path to a known-good state at all.

## Design Contract

- Safety property: no mutation occurs without recovery evidence — either a
  current verified full restore, or a routine admission whose closed effect
  inventory and compatible disaster-recovery receipt are still live at the
  mutation boundary. Untrusted, unreadable, or drifted plans never mutate;
  externally-reaching plans mutate only behind a verified current full
  restore; and a routine admission never creates a physical archive.
- Authority boundary: `src/okg/substrate/deployment_backup_policy.py` owns
  the physical-rollback admission decision (`select_physical_rollback_policy`)
  and the typed effect-inventory / disaster-recovery check parsers, while
  `src/okg/substrate/deployment_upgrade.py` owns effect classification,
  receipt evidence collection, archive creation, restore verification, and
  mutation-time revalidation that consumes the decision.
- Valid runtime paths: refusal on an untrusted/unreadable/drifted plan,
  refusal on a routine plan whose recovery evidence is missing or
  incompatible, routine no-physical-rollback admission then mutation,
  forced archive + verified full restore then mutation, and drift-triggered
  refusal after either admission.
- Out of scope: `pg_dump`/`pg_restore` fidelity, filesystem crash
  consistency, host storage accounting, PostgreSQL command semantics,
  wall-clock latency budgets, and the truthfulness of a receipt's recorded
  observations remain runtime-test and live deployment evidence
  obligations.
- Preconditions/refinement: runtime tests in
  `tests/substrate/test_deployment_upgrade.py` and
  `tests/substrate/test_deployment_backup_budget.py` exercise the concrete
  classification, receipt, archive, restore, and revalidation boundaries;
  `tests/pact/test_deployment_backup_policy_model_contract.py` binds the
  model to the PACT evidence contract and proves each effect-inventory and
  disaster-recovery field is load-bearing.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `DeploymentBackupPolicy.tla` | `DeploymentBackupPolicy.cfg` | Positive model covering plan classification, routine no-physical-rollback admission, forced archive + full-restore verification, target-plan drift revocation, and the mutation gate. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `DeploymentBackupPolicy.tla` | A refresh classifies its target plan, admits a closed routine plan with fresh disaster-recovery proof without any physical backup, otherwise creates and fully restore-verifies a current archive, revalidates at every effect boundary, forces that verified archive for externally-reaching plans, and refuses fail-closed on untrusted, unreadable, recovery-stale, or drifted plans. | `src/okg/substrate/deployment_backup_policy.py`, `src/okg/substrate/deployment_upgrade.py` | `tests/substrate/test_deployment_upgrade.py`, `tests/substrate/test_deployment_backup_budget.py`, `tests/pact/test_deployment_backup_policy_model_contract.py` |

## Invariant Map

| Invariant or property | What it means | Code/test signal |
| --- | --- | --- |
| `TypeInvariant` | Phases, posture flags, effect/recovery failure fields, and the archive/restore/admission/mutation flags stay in the finite modeled domain. | Strict model/report checks in `tests/pact/test_deployment_backup_policy_model_contract.py` and `tests/pact/test_formal_report_producers.py`. |
| `MutationRequiresRecoveryEvidence` | No mutation happens without either a verified full restore or a routine admission whose recovery evidence is compatible. | `test_current_archive_is_required_before_mutation` and admission tests in `tests/substrate/test_deployment_upgrade.py`. |
| `CurrentRestoreRequiresCurrentArchive` | A full restore can only be verified against an archive validated in this run. | Tampered-archive and restore-verification tests in `tests/substrate/test_deployment_upgrade.py`. |
| `PhysicalPlanMutationRequiresCurrentRestore` | Any plan with known physical effects (or outside the dogfood posture) must verify a current full restore before mutating. | `test_schema_or_policy_changes_force_current_restore` and `test_destructive_and_production_paths_require_full_restore` in `tests/pact/test_deployment_backup_policy_model_contract.py`. |
| `RoutineAdmissionRequiresClosedEffects` | Routine no-physical-rollback admission implies the dogfood posture, a fully closed effect inventory, a stable plan, and compatible recovery evidence. | `test_no_physical_rollback_requires_closed_effect_inventory` and `test_every_effect_inventory_field_is_load_bearing` in `tests/pact/test_deployment_backup_policy_model_contract.py`. |
| `EffectClassificationPartitionsFailures` | Every modeled effect is either refused or forces a current archive, never neither and never both. An unclassified effect would make the two external-effect invariants below hold vacuously while TLC still passed. | `test_effect_classification_partitions_every_field` in `tests/pact/test_deployment_backup_policy_model_contract.py`, and the witness run in the deployment-up-external-effects-backup PACT: declassifying `external_effect` makes TLC report `EffectClassificationPartitionsFailures is equal to FALSE`. |
| `UntrustedPlanNeverMutates` | An unpinned, untrusted, unreadable, or unversioned plan refuses before any mutation. | `test_unknown_or_unreadable_plans_refuse` and `test_candidate_planner_must_be_trusted` in `tests/pact/test_deployment_backup_policy_model_contract.py`. |
| `ExternalEffectMutationRequiresCurrentRestore` | A plan whose smoke publish reaches an external service may proceed, but only after verifying a current full restore. Its provider calls cannot be undone; its database still can. | `test_external_effects_force_current_restore` and `test_external_effect_is_a_known_physical_effect_in_the_model` in `tests/pact/test_deployment_backup_policy_model_contract.py`, and `test_external_effect_plan_dumps_instead_of_refusing` in `tests/substrate/test_deployment_upgrade.py`. |
| `ExternalEffectNeverTakesRoutineAdmission` | External reach can never be admitted to the no-physical-rollback path, however good the deployment posture. | `test_external_effects_never_take_the_no_physical_rollback_path` in `tests/pact/test_deployment_backup_policy_model_contract.py`. |
| `TargetPlanDriftNeverMutates` | A target plan that drifts after admission or verification refuses before the mutation boundary. | `test_target_plan_drift_refuses` in `tests/pact/test_deployment_backup_policy_model_contract.py` and drift revalidation tests in `tests/substrate/test_deployment_upgrade.py`. |
| `MissingRoutineRecoveryNeverMutates` | A routine-eligible plan whose disaster-recovery evidence is missing or incompatible refuses instead of creating or skipping a backup. | `test_missing_dr_receipt_refuses_instead_of_dumping` and `test_every_disaster_recovery_field_is_load_bearing` in `tests/pact/test_deployment_backup_policy_model_contract.py`. |
| `NoPhysicalRollbackSkipsCurrentArchive` | A routine admission never creates a physical archive — zero `pg_dump`/`pg_restore` work on the admitted path. | `test_risk_admitted_routine_backup_starts_no_dump_or_restore` and `test_risk_admission_uses_real_target_and_postgres_without_dump` in `tests/substrate/test_deployment_upgrade.py`. |

## Verification Process

Run the positive proof with:

```sh
uv run --extra dev python scripts/check_formal_deployment_backup_policy.py \
  --out .scratch/formal/deployment-backup-policy
```

`DeploymentBackupPolicy.cfg` checks every safety invariant listed above. The
model is registered in `scripts/run_formal_suite.py` as a full-model checker,
so the aggregate suite run

```sh
uv run --extra dev python scripts/run_formal_suite.py --mode smoke
```

exercises this same exhaustive configuration; there is no reduced smoke
profile to diverge from.

## Model Boundary

The model treats each policy decision as atomic and assumes the effect
inventory and disaster-recovery receipt truthfully describe the state they
were computed from. It does not prove `pg_dump`/`pg_restore` fidelity,
archive durability on disk, receipt file integrity against tampering, host
storage accounting, or the wall-clock latency budgets that motivated the
no-physical path. Those remain obligations of the production-bound tests
named in the Transformation Map, of
`scripts/check_deployment_backup_budget.py`, and of live refresh evidence.
