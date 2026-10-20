# Worktree-Branch Safety Formal Model

## System

`WorktreeBranchSafety.tla` specifies the shared safety contract for executing
OKG work in local Git worktrees without allowing preview state, recovery,
lifecycle operations, or activation authority to leak across branches. The
model composes the design obligations owned by the worktree execution-context,
transaction-safety, deterministic-trunk-projection, branch-lifecycle, and
activation PACTs.

The modeled production surface spans branch-scoped graph and control state in
`src/okg/substrate/db/schema.sql`, execution and admission in
`src/okg/substrate/fact_batch.py` and `src/okg/substrate/ingest/runner.py`,
publication in `src/okg/substrate/publisher/publisher.py`, lifecycle authority
in `src/okg/substrate/runtime/admin.py`, and graph reads in
`src/okg/substrate/mcp/server.py`.

## Design Contract

- Safety property: a preview branch cannot mutate `default` or another branch;
  only a complete, current execution capsule may commit; staging is
  non-authoritative; publication and external effects are recoverable and
  at-most-once; clean-trunk projection excludes preview rows; lifecycle and
  activation authority are checked immediately before commit.
- Authority boundary: the database owns branch identity, lifecycle epochs,
  writer epochs, publication compare-and-swap, durable receipts, and committed
  outbox authorization. Git supplies immutable input identity, while a
  controller may present but cannot manufacture database authority.
- Valid runtime paths: reserve, stage, gate, commit, outbox delivery,
  finalization, crash/restart recovery, blocked or superseded termination,
  deterministic clean-trunk projection, refresh, garbage collection,
  controller rollback, same-receipt restart, and scoped activation canaries.
- Out of scope: this bounded model does not prove SQL isolation, DBOS delivery,
  Git merge correctness, source determinism, catalog closure, or external sink
  behavior. Those remain runtime and deployment-proof obligations.
- Preconditions/refinement: `tests/pact/test_worktree_branch_safety_model_contract.py`
  binds every property to the authored PACT fields, model operators, and an
  isolated guard-removal witness. The owning PACTs keep the not-yet-implemented
  disposable-database and live deployment checks explicit instead of treating
  the model as product evidence.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `WorktreeBranchSafety.tla` | `WorktreeBranchSafety.cfg` | Positive composed proof over branch isolation, transaction recovery, projection closure, lifecycle fencing, capability fencing, and fair post-commit completion. |
| `WorktreeBranchSafety.tla` | `WorktreeBranchSafetyDefaultIsolationWitness.cfg` | Removes only `GuardDefaultIsolation`; TLC must refute `DefaultIsolation`. |
| `WorktreeBranchSafety.tla` | `WorktreeBranchSafetyContributionOverlayIsolationWitness.cfg` | Removes only `GuardContributionOverlayIsolation`; TLC must refute `ContributionOverlayIsolation`. |
| `WorktreeBranchSafety.tla` | `WorktreeBranchSafetyTransactionRecoveryWitness.cfg` | Removes only `GuardTransactionRecovery`; TLC must refute `TransactionRecovery`. |
| `WorktreeBranchSafety.tla` | `WorktreeBranchSafetyPostCommitRecoveryWitness.cfg` | Removes committed-outbox authorization after controller rollback; TLC must refute `RollbackPostCommitRecoveryEnabled`. |
| `WorktreeBranchSafety.tla` | `WorktreeBranchSafetyProjectionClosureWitness.cfg` | Removes only `GuardProjectionClosure`; TLC must refute `ProjectionClosure`. |
| `WorktreeBranchSafety.tla` | `WorktreeBranchSafetyLifecycleFencingWitness.cfg` | Removes only `GuardLifecycleFencing`; TLC must refute `LifecycleFencing`. |
| `WorktreeBranchSafety.tla` | `WorktreeBranchSafetyCapabilityFencingWitness.cfg` | Removes only `GuardCapabilityFencing`; TLC must refute `CapabilityFencing`. |

`pact_bindings.yaml` assigns each selected property to exactly one requirement
and intent digest. The checker refuses stale or incomplete bindings before it
runs TLC.

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `WorktreeBranchSafety.tla` | A branch operation validates its immutable capsule, reserves branch-scoped work, stages and gates privately, commits with an expected-parent compare-and-swap, releases durable outbox effects, and finalizes or recovers without leaking state. Clean Git input is reprojected independently; refresh, collection, rollback, and activation change authority under fences. | `src/okg/substrate/db/schema.sql`, `src/okg/substrate/fact_batch.py`, `src/okg/substrate/ingest/runner.py`, `src/okg/substrate/ingest/service.py`, `src/okg/substrate/runtime/source_sync_workflow.py`, `src/okg/substrate/publisher/publisher.py`, `src/okg/substrate/merge_back.py`, `src/okg/substrate/cli/deployment_worktree.py`, `src/okg/substrate/runtime/admin.py`, `src/okg/substrate/mcp/server.py` | `tests/pact/test_worktree_branch_safety_model_contract.py`; the owning PACTs name the required future disposable-database and live deployment tests. |

## Invariant Map

| Invariant | What it means | Code/test signal |
| --- | --- | --- |
| `TypeInvariant` | Every state remains inside the finite branch, authority, transaction, lifecycle, and projection schema. | The positive TLC run and strict report production through `scripts/check_formal_worktree_branch_safety.py`. |
| `DefaultIsolation` | Preview facts never enter `default`, and missing or stale context never falls back to default authority. | `tests/pact/test_worktree_branch_safety_model_contract.py::test_default_isolation_refines_complete_capsule_and_default_guard`. |
| `ContributionOverlayIsolation` | Each branch owns only its contributions and tombstones; branches cannot observe or delete one another's or the immutable base's facts. | `tests/pact/test_worktree_branch_safety_model_contract.py::test_contribution_overlay_refines_branch_qualified_state`. |
| `TransactionRecovery` | Staging is non-authoritative; graph state, control progress, receipts, and outbox effects agree; expected-parent and idempotency reuse are fenced; publication and effects occur at most once. | `tests/pact/test_worktree_branch_safety_model_contract.py::test_transaction_recovery_refines_atomic_publication_and_effects`. |
| `PostCommitRecoveryEnabled`, `RollbackPostCommitRecoveryEnabled` | A durable committed or effect-released operation always retains a recovery step, including after mutable controller authority is rolled back. | The transaction refinement test plus `WorktreeBranchSafetyPostCommitRecoveryWitness.cfg`. |
| `CommittedEventuallyFinalized` | Under `FairSpec`, a committed operation reaches its published terminal receipt and releases its durable-job claim. | `tests/pact/test_worktree_branch_safety_model_contract.py::test_transaction_recovery_refines_atomic_publication_and_effects`. |
| `ProjectionClosure` | Both Git merge orders reproject the same closed clean-trunk input; neither preview rows nor graph-union provenance can land. | `tests/pact/test_worktree_branch_safety_model_contract.py::test_projection_closure_refines_clean_trunk_without_preview_copy`. |
| `LifecycleFencing` | Refresh and collection cannot race in-flight mutations; epoch leases fence stale work; controller rollback changes authority while same-receipt process restart does not. | `tests/pact/test_worktree_branch_safety_model_contract.py::test_lifecycle_fencing_refines_epoch_leases_and_receipt_restart`. |
| `CapabilityFencing` | Publication and external effects require current database-time, digest-bound activation authority while the shared lifecycle fence is held. | `tests/pact/test_worktree_branch_safety_model_contract.py::test_capability_fencing_refines_db_time_and_receipt_authority`. |

Each `WorktreeBranchSafety*Witness.cfg` removes one named guard while leaving
the others enabled. The checker accepts a witness only when TLC refutes the
specific invariant assigned to that guard.

## Verification Process

Run the complete positive proof and all seven isolated guard-removal witnesses:

```sh
uv run python scripts/check_formal_worktree_branch_safety.py \
  --out .scratch/formal/worktree-branch-safety
```

Run a property-owned proof while developing one child PACT:

```sh
uv run python scripts/check_formal_worktree_branch_safety.py \
  --property-id DefaultIsolation \
  --out .scratch/formal/worktree-branch-default-isolation
```

The checker writes `formal-report.yaml` plus witness logs and
`guard-removal-witnesses/witnesses.json`. A passing formal report includes
model/config digests, the exact command, PACT ownership, and refinement-test
bindings. The aggregate runner registers the checker in
`scripts/run_formal_suite.py`; report-shape coverage is classified in
`tests/pact/test_formal_report_producers.py`.

Each witness is also a named aggregate-runner entry. To run only the
post-commit recovery witness (without the positive proof), use:

```sh
uv run python scripts/check_formal_worktree_branch_safety.py \
  --witness-id PostCommitRecovery --witness-only \
  --out .scratch/formal/worktree-branch-post-commit-witness
```

The aggregate `--only` selector accepts either the checker filename or an
exact `WorktreeBranchSafety*Witness.cfg` basename. Unknown selectors and
checkers with no registered witness fail loudly instead of returning a
zero-check `OK`.

## Model Boundary

The state space uses two preview branches, finite fact and contribution sets,
bounded counters, and abstract digests. It assumes the capsule hash, Git commit
identity, catalog closure, external snapshot token, and event-ledger offset are
computed correctly. It also abstracts SQL transactions, queues, DBOS recovery,
clock implementation, external sinks, and the complete deployment controller.
The owning PACTs therefore remain proposals until their real database,
multi-worktree, restart, canary, and live deployment verification succeeds.
