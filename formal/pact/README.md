# PACT Formal Models

## System

The PACT models cover the change-management protocol used by this repository:
manifest mutation, graph projection, strict formal evidence, workflow authority
freshness, and v4 decision-record merge safety.

## Design Contract

- Safety property: PACT workflow progress is authorized only by fresh file,
  graph, evidence, decision, coordination, and git surfaces that agree on the
  current intent.
- Authority boundary: `pact.yaml` is canonical for intent; graph projection is
  read-only context; decision-ledger and git evidence own review/merge claims.
- Valid runtime paths: manifest mutation, graph projection, formal evidence
  ingestion, task/review/merge gates, legacy approvals, and v4 decision records.
- Out of scope: repository merge mechanics, GitHub availability, and graph
  ingestion latency remain live workflow evidence.
- Preconditions/refinement: projection report-property drift and legacy
  approval representation are runtime-test boundaries carried by
  `tests/pact/test_pact_projection_formal.py` and
  `tests/pact/test_pact_v4_decision_ledger_formal.py` (no gap-inventory
  entry exists for them; a prior revision of this line cited one that was
  never written).

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `MutationSafety.tla` | `MutationSafety.cfg` | Manifest mutation and edit-plan replay safety. |
| `PactProjection.tla` | `PactProjection.cfg` | File-to-graph projection freshness and snapshot safety. |
| `PactAgentLifecycle.tla` | `PactAgentLifecycle.cfg`, `PactAgentLifecycleWitness.cfg` | Tier-aware, digest-bound proposal, implementation, review, and retirement authority. |
| `PactFormalEvidence.tla` | `PactFormalEvidence.cfg`, `PactFormalEvidenceStaleAdvanceWitness.cfg`, `PactFormalEvidenceSilentSweepWitness.cfg` | Both halves of the formal-evidence contract: verification advances only on fresh evidence (entry), and evidence that goes stale AFTER verification is flagged by the corpus sweep (detection). One buggy-toggle witness per half refutes the real invariant by name. |
| `PactProofIntegrity.tla` | `PactProofIntegrity.cfg` | Completion requires distinct acceptance-arm witnesses, proven integration owners, and execution-attested named tests. |
| `WorkflowAuthority.tla` | `WorkflowAuthority.cfg` | Current-authority decision rules for task/review/merge progress. |
| `PactV4DecisionLedger.tla` | `PactV4DecisionLedger.cfg` | Ledger-backed approval safety for pre-merge. |
| `MutationSafetyCounterexample.tla` | `MutationSafetyCounterexample.cfg`, `MutationSafetyCompletionWitness.cfg` | Non-vacuity witnesses, one invariant per config (TLC halts at the first violation): the base config proves a plan effect can land; the completion witness proves a mutation can complete. Together the mutation-safety bounds guard reachable state. |
| `WorkflowAuthorityCounterexample.tla` | `WorkflowAuthorityCounterexample.cfg`, `WorkflowAuthorityBlockersWitness.cfg` | Non-vacuity witnesses, one invariant per config: the base config proves a fully-authorized accept is reachable; the blockers witness proves a real runtime blocker is reachable. Together the workflow-authority guards are not vacuous. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `MutationSafety.tla` | Concurrent manifest updates and saved edit plans become a single serialized file mutation — including `okg pact decide`, whose status projection re-loads and saves under `manifest_write_lock` (wave-4 fix: the pre-round-trip snapshot was saved lockless, clobbering concurrent locked verbs). | `src/okg/substrate/pact/store.py`, `src/okg/substrate/pact/operations.py`, `src/okg/substrate/pact/models.py` | `tests/pact/test_cli.py`, `tests/pact/test_formal_report_producers.py` |
| `PactProjection.tla` | File-canonical PACT state is projected into read-only graph rows without letting graph reads mutate the file. | `src/okg/substrate/pact/operations.py`, `src/okg/substrate/pact/validation.py`, `src/okg/substrate/pact/markdown_view.py` | `tests/pact/test_pact_projection_formal.py`, `tests/pact/test_pact_source.py`, `tests/pact/test_formal_report_producers.py` |
| `PactAgentLifecycle.tla` | Structured tier, current intent, task proof, and digest-bound decisions derive one lifecycle state and one legal next boundary. | `src/okg/substrate/pact/fsm.py`, `src/okg/substrate/pact/review.py`, `src/okg/substrate/pact/cli.py` | `tests/pact/test_fsm.py`, `test_v4_lifecycle_authority.py` in the development repository (not in this copy), `tests/pact/test_review.py` |
| `PactFormalEvidence.tla` | Verification advances only while formal evidence is fresh AT ENTRY (the validation gate); evidence can go stale afterwards (model rewrites do not consult lifecycle), and the corpus freshness sweep must then flag it. | `src/okg/substrate/pact/operations.py`, `src/okg/substrate/pact/validation.py`, `src/okg/substrate/pact/cli.py`, `scripts/check_formal_pact_formal_evidence.py` | `tests/pact/test_pact_formal_evidence_formal.py`, `tests/pact/test_v3_formal_flow.py`, `tests/pact/test_formal_binding.py`, `tests/pact/test_evidence_ingest.py`, `tests/pact/test_formal_report_producers.py`, `test_qa_lanes.py` in the development repository (not in this copy) |
| `PactProofIntegrity.tla` | Completion requires one distinct compatible witness per acceptance arm, satisfied integration owners, and execution-attested named tests. | `src/okg/substrate/pact/validation.py`, `src/okg/substrate/pact/operations.py`, `scripts/check_formal_pact_proof_integrity.py` | `tests/pact/test_pact_proof_integrity_formal.py`, `test_acceptance_arm_binding.py` in the development repository (not in this copy), `tests/pact/test_integration_acceptance_validation.py`, `tests/pact/test_slow_verification_commands.py` |
| `WorkflowAuthority.tla` | File freshness, graph freshness, decision records, generation pins, evidence, coordination state, and git evidence become a go/no-go workflow decision. | `src/okg/substrate/pact/workflow_authority.py`, `src/okg/substrate/pact/operations.py`, `src/okg/substrate/pact/cli.py` | `tests/pact/test_workflow_authority_formal.py`, `test_okg_workflow_source_of_truth.py` in the development repository (not in this copy), `tests/pact/test_coordination_gate_evidence.py` |
| `PactV4DecisionLedger.tla` | Human review decisions become pre-merge approval or blockers for stale, reopened, or active file states. | `src/okg/substrate/pact/decisions.py`, `src/okg/substrate/pact/review.py`, `src/okg/substrate/pact/cli.py` | `tests/pact/test_pact_v4_decision_ledger_formal.py`, `tests/pact/test_review.py`, `tests/pact/test_operator_brief_cli.py` |
| `MutationSafetyCounterexample.tla` | A plan effect that lands and a mutation that completes are intentionally asserted unreachable, so TLC must exhibit both, proving the `PlanEffectAtMostOnce` bound and the `NoLostCompletedMutation` durability claim guard reachable, non-trivial state. | `src/okg/substrate/pact/store.py`, `src/okg/substrate/pact/operations.py`, `src/okg/substrate/pact/models.py` | `tests/pact/test_cli.py`, `tests/pact/test_formal_report_producers.py` |
| `WorkflowAuthorityCounterexample.tla` | A fully-authorized accept and a populated runtime-blocker set are intentionally asserted unreachable, so TLC must exhibit both, proving the acceptance side and the `No*Authority` blocker guards have reachable antecedents. | `src/okg/substrate/pact/workflow_authority.py`, `src/okg/substrate/pact/operations.py`, `src/okg/substrate/pact/cli.py` | `tests/pact/test_workflow_authority_formal.py`, `test_okg_workflow_source_of_truth.py` in the development repository (not in this copy), `tests/pact/test_coordination_gate_evidence.py` |

## Invariant Map

| Invariants | What it means | Code/test signal |
| --- | --- | --- |
| `TypeOK` | Manifest state, plans, formal rows, workflow inputs, and decision rows keep the expected shape in each model. | Model binding tests in `tests/pact/test_formal_report_producers.py`. |
| `NoLostCompletedMutation`, `PlanEffectAtMostOnce`, `MutationCriticalLock`, `PlanCriticalLock` | Completed manifest writes cannot be lost, edit plans replay at most once, and mutation/plan critical sections require the manifest lock. | Edit-plan concurrency tests in `tests/pact/test_cli.py`. |
| `GraphOperationsDoNotMutateFile`, `FreshnessStatusReflectsProjection`, `PublishedProjectionIsStateSnapshot`, `FreshGraphCarriesProjectedDependencies`, `FreshGraphCarriesProjectedProofs`, `FreshGraphCarriesLifecycle`, `PactProjectionContract` | DEMOTED to typed documentation (strengthen-formal-models audit): the model's Publish action projects atomically from file state, so these hold by construction and cannot catch projection lag, partial writes, or stale concurrent projections — those remain runtime-test evidence. | `tests/pact/test_pact_projection_formal.py`, `tests/pact/test_pact_source.py`. |
| `TypeOK`, `TierTwoPlusImplementationRequiresProposal`, `ReviewRequiresProvenTasks`, `RetirementRequiresFinalApproval`, `IntentDriftRevokesProgress`, `UnprovenWorkCannotRemainInReview`, `TerminalDecisionsAreArchived`, `PactAgentLifecycleStateContract`, `PactAgentLifecycleApprovalContract`, `PactAgentLifecycleReachabilityWitness` | Tier 0-3 changes cannot cross proposal or final review without a current digest-bound decision, invalid or unproven work cannot remain in review, terminal decisions archive, and the positive retirement path is reachable. | `scripts/check_formal_pact_agent_lifecycle.py`, `tests/pact/test_fsm.py`, `test_v4_lifecycle_authority.py` in the development repository (not in this copy). |
| `VerificationRequiresImplementationOrBypass`, `VerificationEnteredFresh`, `VerificationRequiresSemanticAcceptance`, `VerificationRequiresCompletedTask`, `NoSilentStaleAfterVerification`, `PactFormalEvidenceFresh` | Verification advances only with implementation approval (or explicit bypass), a completed task, semantic acceptance, and FRESH formal evidence at entry (history form — no advance ever happened while stale); evidence that goes stale after verification must be flagged by the corpus sweep, never silently. Witnessed by the two buggy-toggle configs (a gate ignoring freshness; a sweep staying silent). | `tests/pact/test_v3_formal_flow.py`, `tests/pact/test_formal_binding.py`, `tests/pact/test_evidence_ingest.py`, and the corpus sweep `test_formal_evidence_freshness_sweep.py` in the development repository (not in this copy). |
| `CompletionRequiresBoundArms`, `CompletionRequiresIntegrationOwner`, `CompletionRequiresNamedTestExecution`, `ProofIntegrity` | A completed change cannot reuse one generic row across distinct acceptance arms, rely on an unproven integration owner, or cite a named test that did not execute. | `tests/pact/test_pact_proof_integrity_formal.py` plus the mapped adversarial refinement tests. |
| `RuntimeDecisionMatchesBlockers`, `BlockersReflectStaleSurfaces`, `LiveChecksReflectStaleSurfaces`, `FreshAuthorityCanAuthorize`, `NoStaleFileAuthority`, `NoStaleGraphAuthority`, `NoStaleLedgerAuthority`, `NoUnapprovedLedgerAuthority`, `NoUnarchivedApprovalAuthority`, `NoMismatchedGenerationAuthority`, `NoStaleEvidenceAuthority`, `NoCoordinationDisagreementAuthority`, `NoUnauthorizedEventPlaneAuthority`, `NoStaleWorktreeIntentAuthority`, `CodeClaimsRequireGitEvidence`, `ReviewMergeTaskDoneRequireCurrentAuthority`, `WorkflowAuthorityContract` | Workflow progress is allowed only when every required authority surface is current, generation pins match, evidence is fresh, coordination agrees, event-plane authority is authorized, worktree intent is fresh, and code claims have git evidence. The ledger contributes three distinct arms, because each has a different repair: `NoStaleLedgerAuthority` for a ledger the gate could not read, `NoUnapprovedLedgerAuthority` for a ledger that answered that no approval binds the current intent digest (repair: record a decision), and `NoUnarchivedApprovalAuthority` for a current approval on a change that is still unarchived (repair: the archive step, not a decide verdict). | `tests/pact/test_workflow_authority_formal.py`, `test_okg_workflow_source_of_truth.py` in the development repository (not in this copy), `tests/pact/test_coordination_gate_evidence.py`. |
| `LedgerRowsWellFormed`, `RuntimeDecisionMatchesStatus`, `BlockersReflectDecisionRows`, `NoMergeWithoutReachableLedger`, `NoMergeWithoutCurrentApproval`, `NoMergeWithStaleDigest`, `NoMergeAfterReopen`, `NoMergeWithActiveFileState`, `OnlyLatestTerminalApprovalCanMerge`, `ArchivedCurrentApprovalCanMerge`, `PactV4DecisionLedgerContract` | Pre-merge approval must come from the latest reachable decision row for the current digest; stale, reopened, or active file states block merge. | `tests/pact/test_pact_v4_decision_ledger_formal.py`, `tests/pact/test_review.py`, `tests/pact/test_operator_brief_cli.py`. |
| `BrokenPlanNeverApplies`, `BrokenNoMutationCompletes` | Non-vacuity witnesses, one per config: `MutationSafetyCounterexample.cfg` asserts a plan effect never lands and `MutationSafetyCompletionWitness.cfg` asserts a mutation never completes. TLC refutes each by name, proving `PlanEffectAtMostOnce` and `NoLostCompletedMutation` guard reachable, positive state rather than holding vacuously over an empty run. | Each config must report its own named broken invariant violated (`run_formal_suite` matches by name, no any-witness fallback); the positive mutation/plan behavior is exercised by `tests/pact/test_cli.py`. |
| `BrokenAuthorityNeverAccepted`, `BrokenBlockersNeverRaised` | Non-vacuity witnesses, one per config: `WorkflowAuthorityCounterexample.cfg` asserts no state ever clears every blocker and `WorkflowAuthorityBlockersWitness.cfg` asserts no runtime blocker ever arises. TLC refutes each by name, proving `FreshAuthorityCanAuthorize` has a reachable fully-authorized state and the `No*Authority` guards have a reachable non-empty blocker set. | Each config must report its own named broken invariant violated (`run_formal_suite` matches by name); the positive authorize/block behavior is exercised by `tests/pact/test_workflow_authority_formal.py`. |

## Verification Process

Use the dedicated wrappers or Make targets:

```sh
make formal-pact-mutation
make formal-pact-projection
uv run --extra dev python scripts/check_formal_pact_agent_lifecycle.py --out .scratch/formal/pact-agent-lifecycle
make formal-pact-formal-evidence
uv run --extra dev python scripts/check_formal_pact_proof_integrity.py --out .scratch/formal/pact-proof-integrity
make formal-pact-workflow-authority
make formal-pact-v4-decision-ledger
uv run --extra dev python scripts/check_formal_pact_mutation.py
uv run --extra dev python scripts/check_formal_pact_projection.py
uv run --extra dev python scripts/check_formal_pact_formal_evidence.py --out .scratch/formal/pact-formal-evidence
uv run --extra dev python scripts/check_formal_workflow_authority.py
uv run --extra dev python scripts/check_formal_pact_v4_decision_ledger.py
```

`PactFormalEvidence.tla` is exercised by the strict
`scripts/check_formal_pact_formal_evidence.py` wrapper, and the QA pact lane
invokes that wrapper rather than raw TLC. The default report row is the bound
aggregate property `PactFormalEvidenceFresh`; supporting invariants remain
configured TLC obligations and can be emitted explicitly with repeated
`--property-id` options. The formal report ingestion path must reject stale,
sparse, unknown, or self-reported proof rows. Accepted formal evidence is
digest-bound to the reported module, config, checker artifact, and local checker
script inputs; PACT validation recomputes those digests before treating formal
evidence as satisfying requirements, acceptance criteria, task evidence refs, or
verification gates.

## Model Boundary

Implementation tests must exercise the CLI/store paths that mutate manifests,
render graph projections, ingest formal reports, evaluate workflow authority,
and enforce pre-merge decisions. The models do not prove GitHub merge behavior,
Postgres availability, or graph-store correctness; the PACT gates and dogfood
readiness checks provide that live authority.
