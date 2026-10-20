# FactBatch Recovery

## System

This model covers the recovery boundary shared by
`FactBatchApplication`, `FactBatchRecoveryCoordinator`, admission disposition,
and publication. It targets the failure mode where graph application succeeds
but receipt or progress-marker evidence is interrupted, leaving policy or
publication to mistake a partial observation for a terminal result.

## Design Contract

- Safety property: progress markers never lead durable receipt evidence;
  accepted batches have applied receipts; rejected or failed batches have no
  graph effects or compensated receipts; and publication exposes only accepted,
  applied graph state.
- Authority boundary: a Producer authorizes the bounded batch, admission owns
  terminal disposition, receipt recovery owns compensation, and Publisher alone
  owns reader visibility.
- Valid runtime paths: prepare, graph apply, atomic evidence commit, recovery
  replay, acceptance, compensation, rejection/failure, and publication.
- Out of scope: SQL transaction isolation, multi-chunk scheduling, graph-store
  implementation details, and deployment queue behavior remain runtime-test and
  published-generation responsibilities.
- Preconditions/refinement: `tests/substrate/fact_batch/test_recovery.py` and
  `tests/substrate/fact_batch/test_admission_paths.py` drive the corresponding
  production states; no additional preconditions are assumed.

## Model Inventory

- `FactBatchRecovery.tla` models receipt, graph-effect, marker, disposition, and
  publication state transitions.
- `FactBatchRecovery.cfg` checks the six safety invariants over `Spec`.

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `FactBatchRecovery.tla` | Prepare a chunk receipt, apply graph facts, and atomically commit marker/receipt evidence. | `src/okg/substrate/fact_application.py`, `src/okg/substrate/fact_batch.py` | `tests/substrate/fact_batch/test_recovery.py` |
| `FactBatchRecovery.tla` | Fence terminal admission on applied or compensated receipts, then expose only a published accepted generation. | `src/okg/substrate/admission.py`, `src/okg/substrate/publisher/publisher.py` | `tests/substrate/fact_batch/test_admission_paths.py`, `tests/substrate/fact_batch/test_published_equivalence.py` |

## Invariant Map

| Invariant | What it means | Code/test signal |
| --- | --- | --- |
| `MarkerNeverAhead` | A progress marker is durable only after graph application and receipt evidence reach an applied or compensating state. | Recovery fault tests in `tests/substrate/fact_batch/test_recovery.py`. |
| `TerminalAcceptanceSafe` | Admission cannot accept a batch whose receipt is not applied. | Admission lifecycle tests in `tests/substrate/fact_batch/test_admission_paths.py`. |
| `TerminalFailureCompensated` | Rejected and failed dispositions retain no uncompensated graph effect. | Compensation fence test in `tests/substrate/fact_batch/test_admission_paths.py`. |
| `PublishedOnlyAfterAcceptance` | Reader visibility requires accepted admission, applied receipt evidence, and a graph effect. | `tests/substrate/fact_batch/test_published_equivalence.py`. |
| `PreparedBlocksTerminal` | A crash between prepare and evidence commit remains pending for recovery. | Prepared-receipt recovery tests in `tests/substrate/fact_batch/test_recovery.py`. |
| `CompensatingBlocksTerminal` | Admission stays pending until compensation finishes. | Compensation transition tests in `tests/substrate/fact_batch/test_recovery.py` and `tests/substrate/fact_batch/test_admission_paths.py`. |

## Verification Process

Run the strict report producer:

```sh
uv run --extra dev python scripts/check_formal_fact_batch_recovery.py \
  --out .scratch/formal/fact-batch-recovery
```

The checker runs `FactBatchRecovery.tla` with `FactBatchRecovery.cfg`, enforces
a non-vacuity state floor, and emits `formal-report.yaml`. The aggregate formal
suite also executes it through `scripts/run_formal_suite.py`.

Run the implementation refinement tests with:

```sh
uv run --extra dev pytest tests/substrate/fact_batch
```

PACT evidence is ingested with `uv run okg pact evidence ingest <id>
--formal-report <path> --json`.

## Model Boundary

The model intentionally collapses batch contents and multiple chunks to one
boolean graph effect. Runtime tests must continue to prove canonical digests,
profile authority, replay identity, transaction behavior, exact row deltas,
blocked-generation visibility, and queue/DLQ health.
