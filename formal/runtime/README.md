# Runtime Formal Models

## Replaceable workflow cutover

`ReplaceableCutover.tla` models the pre-launch application-version boundary
for exact substrate-owned replaceable workflows. It checks that old
gate-reevaluation rows in `ENQUEUED`, `DELAYED`, or never-started `PENDING`
can be cancelled; current-version, custom, wrong-queue, and started `PENDING`
rows cannot be changed; blocker and launch failures commit no partial cleanup;
and an eligible successful cutover reaches readiness under weak fairness.

The implementation refinement points are
`src/okg/substrate/runtime/services.py` and
`tests/substrate/runtime/test_orphan_schedule_cleanup.py`. Run the exhaustive
model with:

```sh
uv run --extra dev python scripts/check_formal_runtime_cutover.py \
  --out .scratch/formal/runtime-cutover
```

## Deployment apply

## System

`DeploymentApply.tla` models one digest-bound deployment reconciliation across
catalog ownership, catalog application, Nomos baseline activation, source
supervision, and worker reload. It includes stale confirmation, skipped-stage,
duplicate source action, partial failure, and false worker acknowledgment
states that could otherwise make an incomplete deployment look ready.

## Design Contract

- Safety property: Every durable action belongs to the currently confirmed
  desired-state digest, stages execute in order, source registration is unique,
  the worker-acknowledgment digest abstracts the complete desired source and
  schedule set, and ready state means all five stages have converged.
- Authority boundary: `src/okg/substrate/deployment_apply.py` owns planning,
  advisory-lock serialization, receipts, and stage ordering;
  `src/okg/substrate/runtime/worker.py` owns the exact worker acknowledgment.
- Valid runtime paths: a changed plan completes all five stages; a crash or
  interruption resumes from its modeled progress; a failed stage remains
  non-ready; and a changed locked plan refuses the caller's stale confirmation
  before mutation.
- Out of scope: PostgreSQL durability, external process supervision, DBOS queue
  execution, catalog/Nomos internal correctness, and published graph contents
  remain runtime-test and isolated publish-proof obligations.
- Preconditions/refinement: Tests in
  `tests/substrate/cli/test_deployment_apply.py`,
  `tests/substrate/runtime/test_source_supervision_reconcile.py`, and
  `tests/substrate/runtime/test_worker_schedule_modes.py` drive the concrete
  planner, lock, receipt, supervision, and acknowledgment boundaries; the
  isolated proof in `scripts/prove_deployment_apply.py` checks real publication
  and queue effects.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `ReplaceableCutover.tla` | `ReplaceableCutover.cfg` | Application-version cutover safety and progress model covering exact replaceable identity, protected started work, blocker rollback, and readiness after eligible cleanup. |
| `DeploymentApply.tla` | `DeploymentApply.cfg` | Positive deployment-apply safety model covering ordered stages, digest binding, unique source actions, acknowledgment-digest equality, failure visibility, and ready-state convergence. |
| `DeploymentApply.tla` | `DeploymentApplyStalePlanCounterexample.cfg` | Retained witness that permits mutation under a stale confirmed digest and must violate `NoStaleMutation`. |
| `DeploymentApply.tla` | `DeploymentApplySkippedStageCounterexample.cfg` | Retained witness that permits a later stage before its predecessor and must violate `OrderedStages`. |
| `DeploymentApply.tla` | `DeploymentApplyDuplicateActionCounterexample.cfg` | Retained witness that repeats a source-registration action and must violate `NoDuplicateSourceRegistration`. |
| `DeploymentApply.tla` | `DeploymentApplyFalseWorkerAckCounterexample.cfg` | Retained witness that accepts a worker acknowledgment for the wrong source set and must violate `ExactWorkerAck`. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `ReplaceableCutover.tla` | Under the existing deployment and workflow-table locks, classify exact old-version workflow/queue rows, refuse unsafe rows before mutation, cancel eligible rows, and commit readiness only after launch succeeds. | `src/okg/substrate/runtime/services.py`, `src/okg/substrate/runtime/worker.py` | `tests/substrate/runtime/test_orphan_schedule_cleanup.py` |
| `DeploymentApply.tla` | Re-plan under one deployment lock, bind receipts to the locked desired-state digest, execute five ordered stages, and expose only exact convergence as ready. | `src/okg/substrate/deployment_apply.py`, `src/okg/substrate/runtime/worker.py`, `src/okg/substrate/runtime/services.py` | `tests/substrate/cli/test_deployment_apply.py`, `tests/substrate/runtime/test_source_supervision_reconcile.py`, `tests/substrate/runtime/test_worker_schedule_modes.py` |
| `DeploymentApply.tla` counterexample configs | Remove one stale-plan, stage-order, deduplication, or exact-acknowledgment guard so TLC demonstrates that each production guard is load-bearing. | `src/okg/substrate/deployment_apply.py`, `src/okg/substrate/runtime/worker.py` | `tests/pact/test_formal_report_producers.py`, `tests/docs/test_formal_suite_reachability.py` |

## Invariant Map

| Invariant or property | What it means | Code/test signal |
| --- | --- | --- |
| `ReplaceableCutoverSafety` | Only exact old-version replaceable rows can be cancelled; current, custom, wrong-queue, and started-PENDING rows remain protected; blocker and launch failures cannot commit partial cleanup. | State-matrix, nullable-identity, lock, and rollback tests in `tests/substrate/runtime/test_orphan_schedule_cleanup.py`; aggregate report validation in `tests/pact/test_formal_report_producers.py`. |
| `EligibleSuccessfulCutoverLeadsToReady` | Under weak fairness, an eligible cutover whose launch succeeds cannot stutter forever before readiness. | TLC liveness check through `scripts/check_formal_runtime_cutover.py`; real DBOS A-to-B replacement proof in `tests/substrate/runtime/test_orphan_schedule_cleanup.py`. |
| `TypeOK` | Phase, desired/planned digests, completed-stage index, receipt digest, and worker-acknowledgment digest remain inside their explicitly modeled domains. | Formal report validation in `tests/pact/test_formal_report_producers.py`. |
| `OrderedStages` | A deployment cannot execute a stage before every predecessor completed for the same desired state. | Ordered-plan and interruption tests in `tests/substrate/cli/test_deployment_apply.py`; `DeploymentApplySkippedStageCounterexample.cfg` must violate this invariant. |
| `NoStaleReceipt` | A persisted receipt cannot claim a different digest than the active apply. | Receipt binding and drift tests in `tests/substrate/cli/test_deployment_apply.py`. |
| `NoStaleMutation` | A caller confirmed against an obsolete plan cannot mutate after the under-lock re-plan changes. | Concurrent-winner regression in `tests/substrate/cli/test_deployment_apply.py`; `DeploymentApplyStalePlanCounterexample.cfg` must violate this invariant. |
| `NoDuplicateSourceRegistration` | One desired source fingerprint produces at most one stable enqueue action. | Deduplication tests in `tests/substrate/runtime/test_source_supervision_reconcile.py`; `DeploymentApplyDuplicateActionCounterexample.cfg` must violate this invariant. |
| `ExactWorkerAck` | The modeled worker-acknowledgment digest matches the desired digest; that digest abstracts the complete active source/schedule set. | Concrete failure and exact-set tests in `tests/substrate/runtime/test_worker_schedule_modes.py`; `DeploymentApplyFalseWorkerAckCounterexample.cfg` must violate this invariant. |
| `FailureNeverReady` | Any failed or incomplete stage remains a blocking apply finding. | Apply health tests in `tests/substrate/cli/test_deployment_apply.py`. |
| `NoOpMeansConverged` | Despite its historical name, this invariant proves only that ready state has every modeled stage complete under the current desired digest; it does not model writes or an invocation-level no-op. | Write-free replay remains a concrete assertion in `tests/substrate/cli/test_deployment_apply.py` and `scripts/prove_deployment_apply.py`. |
| `DeploymentApplySafety` | The conjunction of ordering, digest, deduplication, acknowledgment, failure, and ready-state convergence guards always holds. | Aggregate formal gate through `scripts/check_formal_deployment_apply.py` and `scripts/run_formal_suite.py`. |

## Verification Process

Run the positive model and all four retained witnesses with:

```sh
uv run --extra dev python scripts/check_formal_deployment_apply.py \
  --out .scratch/formal/deployment-apply
```

Run it through the gating aggregate formal surface with:

```sh
uv run python scripts/qa.py formal
```

The checker writes strict formal-report rows for the positive safety run and
each named counterexample. `tests/docs/test_formal_suite_reachability.py`
requires the checker to remain reachable from the aggregate runner, while
`tests/pact/test_formal_report_producers.py` validates the report shape and
producer inventory.

## Model Boundary

The model treats each stage effect and receipt write as an atomic transition
and assumes observations of catalog, Nomos, DBOS, and worker state are
truthful. It does not prove the external supervisor, database transaction,
source adapter, queue execution, graph projector, safe resume from the terminal
failed state, exact source/schedule-set contents, or invocation-level
write-freedom. Those remain obligations of the production tests named in the
Transformation Map and the isolated published-generation proof.
