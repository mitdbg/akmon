# Deployment Control-Plane Kernel

## System

This bounded TLA+ kernel describes one deployment attempt with immutable
identity, separate lifecycle and database-writer epochs, a transactional
workflow-start outbox, ordered effects and compensation, exact observations,
single-use production approval, crash/restart, projection reads, activation,
publication, and rollback. K0-K13 are the base safety kernel; the remaining
obligations describe the intended external-controller authority boundary.

## Design Contract

- Safety property: one immutable attempt and one writer epoch own each state
  transition; no unobserved effect commits, no unauthorized generation becomes
  visible, rollback restores only the predecessor under new authority, and old
  receipt-derived capabilities cannot mutate after rollback.
- Authority boundary: the external controller ledger is the sole
  lifecycle-association writer. The candidate-independent controller package
  concretely refines authorization, compensation, publication, and rollback;
  current product integration and deployment remain separate proof targets.
- Valid runtime paths: admission refusal, staged and production success,
  exact compensation and rollback for every possibly-applied effect,
  compensation failure block, crash/restart at declared boundaries, approval
  refusal, authorized publication, final observation, predecessor restoration,
  and terminal settlement.
- Out of scope: controller process durability, PostgreSQL transaction details,
  real outbox delivery, producer implementation, graph row effects, queue/DLQ
  health, deployment rollout, and live service availability remain runtime and
  deployment proof obligations.
- Preconditions/refinement: bounded TLC success alone is not runtime proof.
  `controller-refinement.json` AST-inspects the stable controller package and
  executes named rollback tests; the contract suite also binds the model,
  registries, mutations, reachability witnesses, profiles, and intent digests.

K0-K13 remain the imported safety kernel. Additional invariants bind that
kernel to the `externalize-local-okg-lifecycle-controller` intent:
`SoleLedgerAuthority` keeps the external controller ledger as the only writer
of the immutable activation association; `StagedAttemptIsolation` keeps
candidate verification writes non-reader-visible; and
`PublicationVisibilityOrdering` separates ledger authorization, target-database
publication, and final readiness observation. The remaining invariants keep
lifecycle and database-writer epochs distinct, require final observation plus
an active receipt-derived capability before broad mutation, make both recovery
gaps explicit, restore the predecessor only under strictly newer epochs, and
invalidate the old receipt/capability during rollback. Each gap is
crash-reachable. An
authorized-but-unpublished transition is `read_only_stale` with producers
blocked; a published-but-unobserved transition has only derived, not active,
producer capabilities until the ledger records readiness. Reachability
witnesses cover both recovery gaps and rollback; semantic guard-removal
mutations prove each added invariant can fail for its intended fault.
Rollback authorization freezes an exact reverse-order compensation plan under
the rollback receipt. `intended`, `applied`, `observed`, and `committed` effects
are possibly applied and therefore compensated; `planned` effects are excluded.
This rule applies to both reversible and irreversible upgrade effects—the
irreversible marker orders predecessor commitment but does not mean an effect
is non-compensable. Predecessor publication and final rollback observation are
both disabled until every required compensation is independently observed and
committed. Authorization-only recovery uses the same receipt-bound path under a
strictly newer lifecycle epoch while leaving the live target database-writer
epoch unchanged. Only `RestorePredecessorUnderNewEpoch`, after compensation
completes, atomically installs the predecessor generation, rollback receipt, and
strictly newer database-writer epoch. `RollbackWriterEpochRotationBoundary`
proves both sides of that phase boundary. No terminal rollback retains candidate
authority.

`--all` also emits `controller-refinement.json`. That artifact AST-inspects and
digest-binds the formal authorization/publication actions together with the
concrete controller ledger, compensation runner, lifecycle orchestrator,
reference target, and command port. Its phase-boundary obligation verifies that
ledger authorization cannot rotate target writer authority and that the target
publication transaction installs predecessor generation, receipt, and writer
epoch together. It then executes the rollback tests named in the artifact.
`--refinement controller` runs that concrete mapping alone.
`refinement-owners.yaml` keeps the design owners explicit while claiming the
controller mapping actually executed.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `DeploymentControlPlaneKernel.tla` | `DeploymentControlPlaneKernel.cfg` | Bounded safety and recovery kernel exercised across scenario, reachability, and semantic guard-removal registries. |
| `refinement-owners.yaml` | n/a | Names design owners and declares the bounded recovery trace plus concrete controller mapping; it is not live deployment proof. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `DeploymentControlPlaneKernel.tla` | Target transformation for immutable attempt admission, ordered effects, compensation, crash recovery, and lifecycle settlement. | `src/okg/substrate/deployment_authority.py`, `src/okg/substrate/deployment_upgrade.py` | `tests/pact/test_deployment_control_plane_kernel_contract.py::test_typed_scope_is_exact`, `tests/pact/test_deployment_control_plane_kernel_contract.py::test_production_reachability_registry_is_complete` |
| `DeploymentControlPlaneKernel.tla` | Concrete mapping for staged isolation, immutable activation/rollback receipts, exact compensation, publication ordering, and atomic rollback writer-epoch rotation. | `controller/okg_local_controller/ledger.py`, `controller/okg_local_controller/effects.py`, `controller/okg_local_controller/controller.py`, `controller/okg_local_controller/reference_target.py` | `tests/pact/test_deployment_control_plane_kernel_contract.py::test_controller_refinement_predicates_are_concrete_and_mutation_sensitive`, `tests/pact/test_deployment_control_plane_kernel_contract.py::test_authorization_only_recovery_publishes_writer_epoch_atomically` |
| `DeploymentControlPlaneKernel.tla` | Target transformation for product approval, upgrade, promotion, and live deployment composition. | `src/okg/substrate/deployment_promotion.py`, `src/okg/substrate/deployment_upgrade.py` | Product integration and live deployment evidence outside this bounded checker. |

The controller row is an executed concrete source/test mapping. The product
integration row remains a target surface; neither row claims the controller is
installed, authoritative, or healthy in a live deployment.

## Invariant Map

| Invariant or property | What it means | Code/test signal |
| --- | --- | --- |
| `TypeScopeExact` | Every modeled variable and record field remains in the exact bounded domain. | Typed-scope and config checks in `tests/pact/test_deployment_control_plane_kernel_contract.py`. |
| `SingleActiveAttempt`, `SingleWriterEpoch`, `AttemptContextImmutable` | One active attempt has one immutable context and matching writer identity. | Registry and structural contract tests in `tests/pact/test_deployment_control_plane_kernel_contract.py`. |
| `LegalLifecycle` | Lifecycle transitions follow the declared graph and terminal states never reopen. | Lifecycle-edge and reachability tests in `tests/pact/test_deployment_control_plane_kernel_contract.py`. |
| `ObservedCommit`, `IdentityHistoryMonotonic`, `OrderedIrreversibility` | Effects commit only after exact observation and identity reservation; irreversible effects wait for predecessors. | Semantic mutation coverage in `tests/pact/test_deployment_control_plane_kernel_contract.py`. |
| `RecoveryEquivalence` and the bounded recovery property | Every possibly-applied effect is compensated before rollback, compensation failure blocks, and fair bounded recovery settles. | Recovery reachability and mutation registry tests in `tests/pact/test_deployment_control_plane_kernel_contract.py`. |
| `ExactContextHealth`, `ExactSingleUseApproval` | Readiness and production approval match the exact attempt context and are consumed once. | Scenario, approval-refusal, and mutation registries checked by `tests/pact/test_deployment_control_plane_kernel_contract.py`. |
| `ProjectionConsistency`, `CanonicalPathOnly`, `FreshPublication` | Projections mirror authoritative state, legacy mutation paths refuse, and publication uses a new generation. | Reachability and semantic guard-removal tests in `tests/pact/test_deployment_control_plane_kernel_contract.py`. |
| `SoleLedgerAuthority` | Only the external-controller ledger commits lifecycle associations. | Ledger-action structural test in `tests/pact/test_deployment_control_plane_kernel_contract.py`. |
| `StagedAttemptIsolation`, `PublicationVisibilityOrdering` | Candidate writes remain staged until ledger authorization, target publication, and final observation occur in order. | Model-structure and reachability registry tests in `tests/pact/test_deployment_control_plane_kernel_contract.py`. |
| `LifecycleAndDatabaseEpochsAreDistinct` | Lifecycle and database-writer authority cannot alias. | Mutation registry coverage in `tests/pact/test_deployment_control_plane_kernel_contract.py`. |
| `BroadMutationRequiresObservedReceiptCapability` | Broad mutation requires final observation and the current receipt-derived capability. | Guard-removal mutation checks in `tests/pact/test_deployment_control_plane_kernel_contract.py`. |
| `RecoveryGapsAreExplicit` | Authorized-but-unpublished and published-but-unobserved states remain explicit recovery modes. | Production reachability registry completeness test in `tests/pact/test_deployment_control_plane_kernel_contract.py`. |
| `RollbackRestoresPredecessorUnderNewEpoch`, `RollbackWriterEpochRotationBoundary`, `AuthorizationOnlyRollbackSeparatesAuthorizationFromPublication` | Authorization records the rollback receipt and lifecycle epoch without changing live target writer authority; publication atomically installs predecessor, receipt, and the new database-writer epoch. | Authorization/publication phase-boundary tests and targeted mutations in `tests/pact/test_deployment_control_plane_kernel_contract.py`. |
| `TerminalRollbackHasNoCandidateAuthority`, `InvalidatedReceiptCapabilityCannotMutate` | Terminal rollback retains no candidate authority, and an invalidated receipt cannot mutate. | Ledger/action and mutation tests in `tests/pact/test_deployment_control_plane_kernel_contract.py`. |
| `ExactCompensationPlan`, `RollbackReceiptBindsCompensationPlan`, `CompensationOrderIsReverse`, `RollbackPublicationRequiresCompensation`, `RollbackFinalizationRequiresCompensation` | Rollback materializes the exact reverse receipt-bound plan and cannot publish or finalize before compensation commits. | Concrete controller predicates, runtime rollback tests, reachability, and mutation checks. |

## Verification Process

Run the bounded per-PR profile with:

```sh
uv run python scripts/check_formal_deployment_control_plane_kernel.py \
  --profile smoke --out .scratch/formal/deployment-control-plane-kernel-smoke
```

That profile runs named representative safety, recovery, approval, rollback,
reachability, and guard-removal rows. It emits
`kernel-smoke-aggregate.json` with `full_pact_evidence: false`; it does not
write `formal-report.yaml`.

Run the strict full aggregate with:

```sh
uv run python scripts/check_formal_deployment_control_plane_kernel.py \
  --all --out .scratch/formal/deployment-control-plane-kernel
```

The full path runs every safety scenario, reachability witness, semantic
mutation, concrete controller source predicate, and named rollback test before
emitting `kernel-proof-aggregate.json`, `controller-refinement.json`, and
`formal-report.yaml`. `--profile full` is an equivalent scheduled-suite alias;
`--refinement controller` runs only the concrete mapping. Passing output proves
the bounded model and that mapping, not live deployment authority or health.

## Model Boundary

The model collapses a deployment to one environment, attempt, effect,
compensation, approval, and generation lineage. Its recovery settlement claim
is proved only under the declared bounded fairness. The concrete refinement
proves selected source structure and executable rollback behavior; it does not
prove that the controller is installed or authoritative in a live deployment,
that product PostgreSQL and outbox operations compose atomically, that graph
effects were published, or that a local OKG is healthy. Those claims require
product integration tests and live publish/readiness evidence outside this
formal report.
