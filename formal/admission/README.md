# Admission Formal Models

## System

The admission models cover the path from a source or derived producer emitting
facts to those facts becoming eligible for publication. They abstract
`okg_source_sync`, review-mode admission, DBOS workflow attempts, idempotency
keys, derived fact projectability, publication feedback, and terminal rejection
or failure states.

## Design Contract

- Safety property: rejection-blocking, per the add-admission-envelope design's
  "Rejection safety" and "Publish decisiveness" clauses — a rejected, failed,
  or quarantined admission can never make its governed facts publish-visible,
  and a review/quarantine envelope can only publish through an accepted
  decision. Publication itself is DECISIVE, not accepted-only: the publish
  sweep resolves pending-or-accepted envelopes whose refs are under the
  watermark (direct-mode source runs have no review loop and publish from
  pending — the everyday path).
- Authority boundary: admission state in `src/okg/substrate/admission.py` owns
  eligibility; workflow events are telemetry and cannot grant admission.
- Valid runtime paths: direct/pending source admissions swept to published,
  zero-output admissions, derived producer admissions (facts committed BEFORE
  the policy verdict, rejection revoking loadability afterwards), review-mode
  atomic decide-approve, rejected admissions, failed admissions, and publish
  feedback.
- Out of scope: DBOS scheduling, Postgres isolation, queue/DLQ health, and live
  generation row counts remain runtime evidence.
- Preconditions/refinement: the review-sweep exclusion is the publish/block
  sweeps' MODE guard (`admission.py`: review/quarantine envelopes require
  status accepted) — refs attach for ANY mode with output, so the guard is
  load-bearing, exercised by
  `tests/substrate/test_admission_rejection_projection.py::test_publish_sweep_requires_decision_for_review_envelopes`
  and the quarantine isolation tests in
  `tests/substrate/test_admission_quarantine.py`. The derived-fact deny-list
  is LOAD-time only: an envelope rejected or quarantined after its facts
  were checkpointed is not retracted (retraction remains a runtime/test
  boundary). Runtime tests must show that zero-output admissions are
  accepted with no refs, and the model proves that accepted no-ref state
  cannot publish.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `Admission.tla` | `Admission.cfg` | Positive admission lifecycle contract. |
| `Admission.tla` | `AdmissionSmoke.cfg` | CI/operator smoke profile over singleton admissions, runs, derived facts, generations, and idempotency keys. |
| `AdmissionCounterexample.tla` | `AdmissionCounterexample.cfg` | Expected-failing negative coverage for non-vacuity. |
| `AdmissionCounterexample.tla` | `AdmissionFastTrackWitness.cfg` | Non-vacuity witness: a broken model where fast-track skips checks is reachable, proving `FastTrackHasChecks` constrains real state rather than guarding an unreachable precondition. |
| `AdmissionCounterexample.tla` | `AdmissionVisibleDerivedWitness.cfg` | Non-vacuity witness: a broken model where a derived fact becomes visible without an accepted policy is reachable, proving `VisibleDerivedImpliesAcceptedPolicy` and `ExtractorCannotBypassProjectability` are load-bearing. |
| `AdmissionCounterexample.tla` | `AdmissionPublishedVisibleWitness.cfg` | Non-vacuity witness: a published-but-not-visible derived fact is reachable, proving the published-visible derived precondition is not vacuously satisfied. |
| `AdmissionCounterexample.tla` | `AdmissionWorkflowEventWitness.cfg` | Non-vacuity witness: a workflow event attached to a non-accepted envelope is reachable, proving `WorkflowEventNotAuthority` constrains a state that can actually arise. |
| `AdmissionCounterexample.tla` | `AdmissionRejectedFactsWitness.cfg` | Non-vacuity witness: a REJECTED admission with committed facts is reachable (a derived producer rejected after emission), proving the rejection-safety invariants guard a real hazard state rather than a world where rejection with output never happens. |
| `AdmissionCounterexample.tla` | `AdmissionPendingPublishWitness.cfg` | Non-vacuity witness: a pending envelope publishes with no accepted event (the direct-mode publish-decisiveness sweep), proving the pending-to-published transition is genuinely exercised. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `Admission.tla` | Producer output is wrapped in an admission envelope, then classified as accepted, rejected, published, or failed. | `src/okg/substrate/admission.py`, `src/okg/substrate/source_sync.py` | `tests/substrate/test_derived_producer_authority.py`, `tests/substrate/test_ingest_service.py` |
| `Admission.tla` | A source run with zero observed output is accepted without scanning or inserting admission refs. That accepted state is not publishable unless refs are later attached, which the zero-output path does not do. | `src/okg/substrate/ingest/service.py::_finalize_source_admission`, `src/okg/substrate/ingest/service.py::_zero_output_admission_verdict`, `src/okg/substrate/admission.py::attach_fact_range_refs_for_run` | `tests/substrate/test_ingest_service.py::test_zero_output_source_sync_uses_bounded_admission_path` |
| `Admission.tla` | The publish sweep marks pending-or-accepted envelopes with watermark-covered refs as published (`PublishFeedback` from either status); refs attach for any admission mode with output (`CommitFacts`/`RecoverRefs`). | `src/okg/substrate/admission.py::_mark_generation_published`, `src/okg/substrate/ingest/service.py::_finalize_source_admission`, `src/okg/substrate/admission_conformance.py` | `tests/substrate/test_derived_producer_authority.py`, `tests/substrate/test_ingest_service.py` |
| `Admission.tla` | A derived producer's facts are committed before its policy verdict (`EmitDerivedFacts` then `EvaluateDerivedPolicy`/`RejectDerivedPolicy`); rejection revokes loadability, enforced by the derived-fact loader's (source, run_id) deny-list for rejected/failed/quarantined envelopes. | `src/okg/substrate/publisher/publisher.py::_load_unapplied_derived_facts`, `src/okg/substrate/publisher/publisher.py` (derived admission flow) | `tests/substrate/test_admission_rejection_projection.py` |
| `Admission.tla` | Review/quarantine envelopes reach published only through an accepted decision: the publish and blocked sweeps carry a mode guard (review/quarantine require status accepted), and decide-approve emits facts, attaches refs, and accepts atomically (`ApproveReview`). | `src/okg/substrate/admission.py::_mark_generation_published`, `src/okg/substrate/runtime/admission_workflows.py` | `tests/substrate/test_admission_rejection_projection.py::test_publish_sweep_requires_decision_for_review_envelopes`, `tests/substrate/test_admission_quarantine.py` |
| `Admission.tla` | DBOS workflow events record attempts and recovery windows, but do not grant admission authority. | `src/okg/substrate/admission.py`, `src/okg/substrate/source_sync.py` | `tests/substrate/test_admission_workflow_ref.py` |
| `Admission.tla` | The smoke profile reruns the same positive safety invariants over singleton universes for CI/operator freshness accounting. | `src/okg/substrate/admission.py`, `src/okg/substrate/ingest/service.py` | `tests/pact/test_formal_report_producers.py::test_expensive_tlc_smoke_profiles_emit_profile_metadata` |
| `AdmissionCounterexample.tla` | Broken publish behavior is intentionally reachable so the positive model is not vacuous. | `src/okg/substrate/admission.py`, `src/okg/substrate/publisher/publisher.py` | `tests/substrate/test_derived_producer_authority.py` |

## Invariant Map

| Invariants | What it means | Code/test signal |
| --- | --- | --- |
| `TypeOK` | Every envelope, ref, event, and generation field stays inside the modeled state shape. | Schema-facing helpers in `src/okg/substrate/admission.py` and admission setup in `tests/substrate/test_ingest_service.py`. |
| `NoDuplicateIdempotency` | Replaying the same producer key must coalesce; it must not create a second independent envelope. | Idempotency cases in `tests/substrate/test_derived_producer_authority.py`. |
| `FastTrackHasChecks` | Fast-track admission is still checked; it is not a bypass around producer authority. | Fast-track producer policy tests in `tests/substrate/test_derived_producer_authority.py`. |
| `ZeroOutputAcceptedHasNoRefs`, `ZeroOutputAdmissionCannotPublishWithoutRefs` | A zero-output run may be accepted with no committed facts and no admission refs, but that no-ref state cannot become published. | `tests/substrate/test_ingest_service.py::test_zero_output_source_sync_uses_bounded_admission_path` checks the accepted status, empty ref table, zero record counts, and `zero_output_run` verdict reason. |
| `RefsBeforePublish`, `PublishedHaveGeneration`, `RejectedNeverPublished`, `RejectedNeverLaterPublished` | Publication requires attached refs and a real generation; a rejected envelope can never gain a published event afterwards (history form — holds through Reject firing only from pending, the publish sweep excluding rejected, and rejected being terminal). | Publish feedback and rejection assertions in `tests/substrate/test_derived_producer_authority.py`; rejection-projection mechanisms in `tests/substrate/test_admission_rejection_projection.py`. |
| `ReviewPublishRequiresDecision` | A review/quarantine envelope that is published must carry an accepted decision event — the publish-decisiveness sweep cannot bypass review, because the sweep's mode guard requires review/quarantine envelopes to be accepted (deleting that guard from `PublishFeedback`/`BlockFeedback`, or from the production sweeps, trips this). | `tests/substrate/test_admission_rejection_projection.py::test_publish_sweep_requires_decision_for_review_envelopes` (fails on the pre-guard sweep); quarantine isolation tests in `tests/substrate/test_admission_quarantine.py`. |
| `EventsReferenceKnownEnvelope`, `WorkflowAttemptsAreNamed`, `WorkflowEventNotAuthority` | Workflow telemetry must name a real envelope and workflow attempt, but telemetry alone cannot accept or publish data. | `tests/substrate/test_admission_workflow_ref.py` and source-sync event tests in `tests/substrate/test_ingest_service.py`. |
| `VisibleDerivedImpliesPublishedEnvelope`, `RejectedDerivedNeverVisible`, `ExtractorCannotBypassProjectability` | A visible derived fact's producing envelope is published (renamed from VisibleDerivedImpliesAcceptedPolicy: under publish decisiveness a producer whose evaluation never completed still publishes from pending, so visibility does not imply affirmative acceptance); rejected/failed/quarantined producer runs stay hidden via the loader deny-list. | Derived producer authority tests in `tests/substrate/test_derived_producer_authority.py`; the loader deny-list regression test in `tests/substrate/test_admission_rejection_projection.py`. |
| `BrokenNoPublish` | The negative model must find a broken trace where publish safety fails. | `make formal-admission-counterexample` must produce the expected counterexample artifact. |
| `BrokenNoFastTrack`, `BrokenNoVisibleDerived`, `BrokenNoPublishedVisibleDerived`, `BrokenNoWorkflowEventOnNonAccepted`, `BrokenNoRejectedWithCommittedFacts`, `BrokenPublishRequiresAcceptedEvent` | Per-witness non-vacuity invariants over `AdmissionCounterexample.tla`, one per config. Each is intentionally refuted by TLC by name, proving that the precondition of a positive invariant (`FastTrackHasChecks`, `VisibleDerivedImpliesPublishedEnvelope`/`ExtractorCannotBypassProjectability`, the published-visible derived case, `WorkflowEventNotAuthority`, the rejected-with-committed-facts hazard state behind the rejection-safety pair, and the pending-to-published publish-decisiveness sweep) is reachable rather than tautologically satisfied. | `AdmissionFastTrackWitness.cfg`, `AdmissionVisibleDerivedWitness.cfg`, `AdmissionPublishedVisibleWitness.cfg`, `AdmissionWorkflowEventWitness.cfg`, `AdmissionRejectedFactsWitness.cfg`, and `AdmissionPendingPublishWitness.cfg` must each report their own named broken invariant violated (`run_formal_suite` matches by name); the same positive behavior is exercised by `tests/substrate/test_derived_producer_authority.py`, `tests/substrate/test_admission_workflow_ref.py`, and `tests/substrate/test_admission_rejection_projection.py`. |

## Execution Profiles

| Profile | Config | Budget | Purpose | Command |
| --- | --- | --- | --- | --- |
| `smoke` | `AdmissionSmoke.cfg` | 30s | CI/operator freshness check; not release evidence. | `uv run --extra dev python scripts/check_formal_admission.py --profile smoke --out .scratch/formal/admission-smoke --timeout-seconds 30` |
| `full` | `Admission.cfg` | 900s | Full-strength release/PACT evidence. | `uv run --extra dev python scripts/check_formal_admission.py --profile full --out .scratch/formal/admission --timeout-seconds 900` |

Use `uv run --extra dev python scripts/check_formal_admission.py
--list-profiles --json` to inspect the profile metadata. If a run times out,
the report means `timeout/not freshly proven`; it is not a semantic invariant
failure.

## Verification Process

Run the positive and negative checks through the repository targets:

```sh
make formal-admission-bootstrap
make formal-admission
make formal-admission-counterexample
make verify-admission
```

The checker uses the pinned TLC jar at `.tools/tla2tools.jar`, `$TLA2TOOLS_JAR`,
or `tools/tla2tools.jar`. Missing Java or TLC is a hard failure. Artifacts are
written under `.scratch/formal/admission` by default, including stdout/stderr
logs and a counterexample artifact for the expected-failing model.

When this model changes, produce evidence with
`scripts/check_formal_admission.py` or the Make targets above and attach the
result to the relevant change record.

## Model Boundary

The model does not prove DBOS, Postgres, or Python implementation correctness.
Implementation tests must prove that source-sync fact insertion, admission ref
insertion, derived fact projectability, idempotency coalescing, and publish
eligibility follow the same state distinctions. Publication health, generation
row counts, and queue/DLQ behavior remain live substrate evidence, not TLA
evidence.
