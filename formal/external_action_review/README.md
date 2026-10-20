# External Action Review

## System

This model covers OKG review of an external effect. OKG stores the proposal,
decision, claim, outcome, and audit state but never calls the provider.

## Design Contract

- Safety property: review decisions bind the exact current lifecycle, payload
  version, and payload hash; proposer and approver subjects differ; approval
  expiry prevents a first claim; claim and terminal outcome are unique; replay
  resolves its authenticated owner; and `providerCalls` always remains zero.
- Authority boundary: authenticated principals and strict Nomos policy authorize
  OKG ledger transitions, while only the external worker and effect ledger own
  provider execution.
- Valid runtime paths: submit, edit, approve, reject, cancel, expire, claim,
  report one terminal outcome, and exact authenticated replay.
- Out of scope: identity-provider verification, PostgreSQL transaction
  isolation, signed receipts, restore inventories, external worker journaling,
  and the external effect ledger remain runtime obligations.
- Preconditions/refinement: the production service, PostgreSQL, API, and
  separate-process fake-worker tests named below must pass; the formal model
  does not replace those runtime checks.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `ExternalActionReview.tla` | `ExternalActionReview.cfg` | Bounded review, claim, outcome, expiry, and replay lifecycle. |
| `ExternalActionReview.tla` | `ExternalActionReviewStaleDecisionWitness.cfg` | Removing complete decision fencing must violate review safety. |
| `ExternalActionReview.tla` | `ExternalActionReviewExpiredClaimWitness.cfg` | Weakening the strict claim deadline must violate execution safety. |
| `ExternalActionReview.tla` | `ExternalActionReviewStaleCancelWitness.cfg` | Removing lifecycle/decision cancellation fencing must violate review safety. |
| `ExternalActionReview.tla` | `ExternalActionReviewReplayOwnerWitness.cfg` | Returning replay before owner resolution must violate execution safety. |

## Transformation Map

| Runtime transformation | Production code | Implementation tests |
| --- | --- | --- |
| `ExternalActionReview.tla`: submit, edit, decision, expiry, and cancellation | `src/okg/substrate/external_actions/service.py` and `store.py` | `tests/substrate/external_actions/test_service.py`, `test_store_postgres.py` |
| `ExternalActionReview.tla`: claim, report, and exact replay | `src/okg/substrate/external_actions/service.py` and `api.py` | `tests/substrate/external_actions/test_api.py`, `tests/e2e/test_external_action_review.py` |

## Invariant Map

| Invariant | What it means | Code/test signal |
| --- | --- | --- |
| `ExternalActionReviewSafety` | Original payload immutability, complete stale-decision and cancellation fencing, separated approval, and zero provider calls. | Core service and PostgreSQL race tests. |
| `ExternalActionExecutionSafety` | Only the approved effect is claimed, claim/outcome are unique, deadline and replay ownership are enforced, and provider calls remain zero. | Claim/outcome API and fake-worker tests. |

## Verification Process

Run:

```sh
uv run --extra dev python scripts/check_formal_external_action_review.py \
  --out .scratch/formal/external-action-review
```

The checker runs the baseline and four guard-removal witnesses, writes their
TLC logs, and emits `formal-report.yaml` for PACT ingestion. Its PACT binding
uses stable production paths, while the checker verifies the model bytes
against whichever single active or archived design copy exists.

## Model Boundary

The model deliberately abstracts canonical JSON, authentication providers,
Nomos storage, database locks, event hashes, signatures, worker journaling,
and restore reconciliation. Passing TLC is not proof that a provider effect
ran, and no modeled transition grants OKG provider access.
