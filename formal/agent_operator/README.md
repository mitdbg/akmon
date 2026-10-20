# Agent/operator runtime safety

## System

`AgentOperatorSafety.tla` models three daily operator paths: bounded automatic
approval of review admissions, coordination between watch-triggered and
poll-triggered source sync, and cleanup of abandoned source-sync workflows. A
bad transition can accept an unsafe payload, bill the same work twice, suppress
a due reconciliation, or delete work that has already started.

## Design Contract

- Safety property: automatic approval accepts only the complete eligible case;
  an active watch suppresses an ordinary poll without advancing its baseline;
  baseline advancement requires a durable suppression receipt and exact applied
  token; a due backstop still enqueues; and automatic cleanup never reaps started
  work.
- Authority boundary: the model owns the admission decision, trigger outcome,
  baseline-advance flag, workflow-start state, and cleanup decision. Production
  configuration and persisted DBOS state remain authoritative inputs.
- Valid runtime paths: admission may remain pending or accept the eligible
  case; watch work may complete or fail before a poll; a due backstop may
  enqueue; and only a never-started workflow may be reaped automatically.
- Out of scope: wall-clock duration, DBOS queue transport, PostgreSQL locking,
  graph projection, operator-authentication policy, and manual cleanup are
  checked by runtime and isolated-publication tests rather than this model.
- Preconditions/refinement: `tests/substrate/runtime/test_admission_workflows.py`,
  `tests/substrate/test_source_sync_poll_bounded.py`, and
  `tests/substrate/runtime/test_worker_schedule_modes.py` exercise the same
  decisions against production helpers and persisted workflow state.

## Model Inventory

| Model | Configurations | Purpose |
| --- | --- | --- |
| `AgentOperatorSafety.tla` | `AgentOperatorSafety.cfg`, `AgentOperatorUnsafeApproval.cfg`, `AgentOperatorDuplicatePoll.cfg`, `AgentOperatorBackstopSuppression.cfg`, `AgentOperatorStartedReap.cfg` | Checks the positive safety kernel and four exact negative witnesses. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `AgentOperatorSafety.tla` | Classify and accept a bounded review admission; arbitrate watch, ordinary-poll, and due-backstop work; reap only never-started recovered workflows. | `src/okg/substrate/runtime/admission_workflows.py`, `src/okg/substrate/source_sync_producers.py`, `src/okg/substrate/runtime/worker.py` | `tests/substrate/runtime/test_admission_workflows.py`, `tests/substrate/test_source_sync_poll_bounded.py`, `tests/substrate/runtime/test_worker_schedule_modes.py` |

## Invariant Map

| Invariant | What it means | Code/test signal |
| --- | --- | --- |
| `TypeOK` | Every modeled field stays inside its declared state set. | The checker fails before any evidence row passes. |
| `ApprovalIsBounded` | An accepted admission must be the eligible case. | Runtime admission refusal tests and `AgentOperatorUnsafeApproval.cfg`. |
| `ActiveWatchSuppressesOrdinaryPoll` | Ordinary poll cannot enqueue or advance its baseline while a non-backstop watch is active. | Source-sync poll tests and `AgentOperatorDuplicatePoll.cfg`. |
| `BackstopRetainsAuthority` | A due backstop is enqueued even when event-driven dedupe exists. | Due-backstop tests and `AgentOperatorBackstopSuppression.cfg`. |
| `BaselineAdvanceRequiresAppliedDelegation` | A completed watch advances the poll baseline only when a persisted suppression receipt exists and the watch applied that exact observed token. | Exact-token and newer-token source-sync poll regressions. |
| `StartedWorkflowIsNotReaped` | Automatic cleanup cannot delete a started workflow. | Worker cleanup tests and `AgentOperatorStartedReap.cfg`. |
| `AgentOperatorRuntimeSafety` | The type, approval, trigger, backstop, and cleanup invariants hold together. | Aggregate checker failure or any bound refinement-test failure. |

## Verification Process

Run the checker directly with:

```bash
uv run --extra dev python scripts/check_formal_agent_operator_ergonomics.py \
  --out .scratch/formal/agent-operator
```

Run the checker directly to emit `.scratch/formal/agent-operator/formal-report.yaml`.
The repository gate, `uv run python scripts/qa.py formal`, stores the aggregate
copy at
`.scratch/formal/suite/smoke/check_formal_agent_operator_ergonomics/formal-report.yaml`.
Both forms must contain eight fresh strict rows: four passing safety rows and
four expected-witness rows, each bound to its config and refinement tests.

## Model Boundary

The model deliberately collapses payload contents to an eligibility class,
watch execution to four states, scheduling to a due-backstop boolean, and DBOS
execution to never-started versus started. Configuration parsing, real
PostgreSQL/DBOS workflow rows, queue/DLQ health, published graph rows, replay
stability, CLI rendering, and Chronos derivation output remain covered by the
runtime tests and `scripts/prove_agent_operator_ergonomics.py`.
