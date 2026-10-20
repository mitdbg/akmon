# Targeted Enricher Backfill Formal Model

## System

This model covers one invocation of targeted enricher backfill. The command
selects one registered enricher, checks that it is safe for chunked replay,
processes a bounded set of missing input nodes, records per-input progress, and
advances normal enricher progress only after the selected worklist drains
without failed inputs.

## Design Contract

- Safety property: targeted backfill may run only the selected enricher, may
  write only accepted in-scope edge facts for the current chunk, and may advance
  global enricher progress only after all selected inputs are complete with no
  failed inputs.
- Authority boundary: `src/okg/substrate/enrichers/backfill.py` owns selection,
  branch support, chunk-safety checks, chunk execution, progress writes, and
  progress advancement. CLI parsing only supplies command inputs.
- Valid runtime paths: missing enricher selection, ambiguous selection,
  unsupported branch, unsafe enricher, out-of-scope output, accepted chunk,
  rejected chunk, retryable failed inputs, partial completion, and full
  completion.
- Out of scope: normal publish scheduling, queue health, Postgres isolation,
  ontology-specific edge semantics, and non-default branch progress remain
  implementation and deployment-test concerns.
- Preconditions/refinement: No additional preconditions. Runtime tests must
  prove that selection fails before writes, only chunk-safe edge enrichers are
  accepted, failed chunks stay retryable, out-of-chunk writes are rejected, and
  normal enricher progress advances only after the missing-input worklist is
  drained.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `TargetedEnricherBackfill.tla` | `TargetedEnricherBackfill.cfg` | Positive targeted backfill isolation, chunk progress, retry, and progress-advancement contract. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `TargetedEnricherBackfill.tla` | Command input selects exactly one registered enricher before any backfill writes. Missing or ambiguous names stop without graph writes. | `src/okg/substrate/enrichers/backfill.py::select_enricher`, `src/okg/substrate/cli/main.py` | `tests/substrate/cli/test_enricher_backfill_cli.py::test_enricher_backfill_cli_selects_one_registered_enricher`, `tests/substrate/cli/test_enricher_backfill_cli.py::test_enricher_backfill_cli_rejects_duplicate_names_before_writes` |
| `TargetedEnricherBackfill.tla` | The command rejects non-default branches and enrichers that are not explicitly marked chunk-safe. | `src/okg/substrate/enrichers/backfill.py::run_enricher_backfill`, `src/okg/substrate/enrichers/backfill.py::_require_chunk_safe_enricher` | `tests/substrate/test_enricher_backfill.py::test_backfill_rejects_non_default_branch_before_progress`, `tests/substrate/test_enricher_backfill.py::test_backfill_rejects_unmarked_enricher_before_writes` |
| `TargetedEnricherBackfill.tla` | Missing inputs are processed in bounded chunks and recorded in `okg.enricher_backfill_progress`. | `src/okg/substrate/enrichers/backfill.py::_missing_input_ids`, `src/okg/substrate/enrichers/backfill.py::_run_chunk`, `src/okg/substrate/enrichers/backfill.py::_mark_inputs_complete` | `tests/substrate/test_enricher_backfill.py::test_backfill_processes_limit_in_resumable_chunks` |
| `TargetedEnricherBackfill.tla` | Rejected chunks do not advance global progress, and failed inputs remain available for retry. | `src/okg/substrate/enrichers/backfill.py::_run_backfill_locked`, `src/okg/substrate/enrichers/backfill.py::_run_chunk` | `tests/substrate/test_enricher_backfill.py::test_backfill_rejected_admission_does_not_advance_progress` |
| `TargetedEnricherBackfill.tla` | The command rejects facts emitted outside the current chunk and advances normal enricher progress only after no missing inputs remain. | `src/okg/substrate/enrichers/backfill.py::_validate_chunk_edge_scope`, `src/okg/substrate/enrichers/backfill.py::_run_backfill_locked` | `tests/substrate/test_enricher_backfill.py::test_backfill_rejects_same_source_out_of_chunk_edges`, `tests/substrate/test_enricher_backfill.py::test_backfill_satisfies_missing_enricher_progress_for_publish_skip`, `tests/substrate/test_enricher_backfill.py::test_normal_publish_skips_completed_targeted_backfill_first_run` |

## Invariant Map

| Invariants | What it means | Code/test signal |
| --- | --- | --- |
| `TypeOK` | Selection state, branch state, chunk bounds, input sets, lock state, phase, and graph-write flags stay inside the modeled state shape. | Formal checker plus setup coverage in `tests/substrate/test_enricher_backfill.py`. |
| `TargetedBackfillIsolation` | Missing, ambiguous, non-default branch, and unsafe-enricher cases write nothing; valid runs invoke only the selected enricher and no unrelated surfaces. | CLI selection tests and `tests/substrate/test_enricher_backfill.py::test_backfill_rejects_unmarked_enricher_before_writes`. |
| `ChunkProgressRetrySafety`, `PinnedFrontierSafety` | Chunks respect `limit` and `chunk_size`; completed and failed inputs are known, disjoint, and processed; failed inputs stay retryable; out-of-scope outputs never complete; global progress advances only from a safe pinned frontier after selected work drains. | Chunking, rejected-admission, progress-advancement, and out-of-chunk tests in `tests/substrate/test_enricher_backfill.py`. |
| `TargetedEnricherBackfillContract` | Combined positive contract over type safety, isolation, chunk progress, retry safety, locking, and progress advancement. | `scripts/check_formal_enricher_backfill.py` emits strict formal report rows for the bound properties. |

## Verification Process

Run:

```sh
uv run --extra dev python scripts/check_formal_enricher_backfill.py \
  --out .scratch/formal/targeted-enricher-backfill
```

The checker uses `.tools/tla2tools.jar`, `$TLA2TOOLS_JAR`, or
`tools/tla2tools.jar` and fails loudly when Java/TLC is missing. The strict
formal report binds each property to the runtime tests listed above. Those tests
must pass when the model or implementation changes:

```sh
uv run --extra dev pytest \
  tests/substrate/cli/test_enricher_backfill_cli.py \
  tests/substrate/test_enricher_backfill.py
```

## Model Boundary

The model does not prove SQL isolation, publisher scheduling, queue behavior, or
deployment-specific enricher correctness. Implementation tests must prove that
the Python command enforces the same selection, chunk-safety, chunk-scope,
retry, and progress-advancement distinctions against real database state.
