# Library Configuration Admission Formal Model

## System

This model covers the strict library-configuration admission boundary. A
document can describe multiple sources, so schema and semantic validation cover
every entry and constructor-binding validation plans every runnable entry before
selection. Only after that whole-document barrier may selected adapters be
constructed, frozen, allowed to mutate state, or run.
The model targets the partial-start failure mode where an earlier valid source
starts before a later invalid source causes the same document to be refused.

## Design Contract

- Safety property: construction, frozen bindings, mutations, and execution are
  possible only after every entry in the document passes every admission
  phase; a refused document has none of those effects, and executed entries
  use frozen strict bindings.
- Authority boundary: `src/okg/substrate/sources/registry.py` owns document
  validation, `src/okg/substrate/ingest/adapter_factory.py` owns constructor
  planning and frozen bindings, and the ingest and polling services consume
  only the admitted plan.
- Valid runtime paths: entries may pass or fail schema, semantic, or binding
  validation in any order; the document barrier opens only after all entries
  pass; admitted entries may then be constructed, frozen, mutate state, and
  execute.
- Out of scope: YAML parsing, JSON Schema implementation details, adapter
  business logic, database transaction ordering after the barrier, publication
  correctness, and queue health remain runtime-test and isolated-publish
  obligations.
- Preconditions/refinement: `Entries` is a finite non-empty document with two
  representative sources in the checked configurations. Production refinement
  is exercised by `tests/substrate/test_source_registry_contract.py`,
  `tests/substrate/test_adapter_factory.py`,
  `tests/substrate/test_ingest_service.py`, and
  `tests/substrate/test_source_sync_poll_bounded.py`.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `LibraryConfigAdmission.tla` | `LibraryConfigAdmission.cfg` | Positive safety model: the document barrier opens only after all entries pass schema, semantic, and binding validation, after which construction and execution may proceed. |
| `LibraryConfigAdmission.tla` | `LibraryConfigAdmissionCounterexample.cfg` | Guard-removal witness: `BuggyPartialStart = TRUE` permits an already validated entry to construct before the whole document reaches the barrier and must refute `AllSideEffectsRequireDocumentBarrier`. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `LibraryConfigAdmission.tla` | The loader validates the complete strict source document and returns immutable entries only when schema and semantic checks all pass. | `src/okg/substrate/sources/registry.py` | `tests/substrate/test_source_registry_contract.py` |
| `LibraryConfigAdmission.tla` | Constructor signatures and authored parameters are planned for every runnable entry in the marked document before selection; only selected adapters are then constructed and frozen. | `src/okg/substrate/ingest/adapter_factory.py`, `src/okg/substrate/ingest/service.py` | `tests/substrate/test_adapter_factory.py`, `tests/substrate/test_ingest_service.py` |
| `LibraryConfigAdmission.tla` | Polling consumes the same whole-document plan before poll-state reads, queue writes, or selected-source execution. | `src/okg/substrate/source_sync_producers.py` | `tests/substrate/test_source_sync_poll_bounded.py` |

## Invariant Map

| Invariants | What it means | Code/test signal |
| --- | --- | --- |
| `TypeOK` | Every phase, checked-entry set, failure marker, barrier flag, and effect set remains within the declared finite domains. | Strict checker output from `scripts/check_library_config_admission_model.py` and report-shape coverage in `test_prove_library_config_core_integration.py` in the development repository (not in this copy). |
| `AllSideEffectsRequireDocumentBarrier` | Construction, freezing, mutation, or execution implies that every document entry passed schema, semantic, and binding validation and the barrier opened. | Multi-entry refusal and constructor-barrier tests in `tests/substrate/test_source_registry_contract.py`, `tests/substrate/test_ingest_service.py`, and `tests/substrate/test_source_sync_poll_bounded.py`. |
| `RefusedDocumentHasNoSideEffects` | Once any entry refuses the document, no entry was constructed or frozen and no mutation or execution occurred. | Refusal-side-effect assertions in `tests/substrate/test_source_registry_contract.py` and `tests/substrate/test_source_sync_poll_bounded.py`. |
| `ExecutedEntriesUseFrozenStrictBindings` | Every executed strict source is a member of the frozen binding set created after whole-document admission. | Binding-authority tests in `tests/substrate/test_adapter_factory.py` and runner tests in `tests/substrate/test_ingest_service.py`. |
| `LibraryConfigAdmissionSafety` | Type safety and all three admission guarantees hold together for every reachable positive-model state. | The positive TLC run and the focused registry, binding, ingest, and polling refinement suites. |
| `LibraryConfigAdmissionCounterexample.cfg` expected counterexample | Removing the barrier lets a source construct and freeze before a later entry is admitted or refused, so TLC must refute `AllSideEffectsRequireDocumentBarrier`. | Counterexample enforcement in `scripts/check_library_config_admission_model.py`. |

## Verification Process

Run:

```sh
uv run --extra dev python scripts/check_library_config_admission_model.py
```

The checker runs `LibraryConfigAdmission.cfg` as the passing safety proof and
`LibraryConfigAdmissionCounterexample.cfg` as the required guard-removal
witness. It writes the PACT-bound formal report under
`.scratch/formal/library-config-admission/`. Attach the report through the
`canonicalize-library-configuration-admission-core` PACT evidence workflow.

## Model Boundary

The model deliberately represents each validation phase and downstream effect
as a finite set transition. It does not prove the JSON Schema vocabulary,
Python signature introspection, adapter-specific behavior, database commits,
publication, or rollback. Those details remain covered by focused runtime
tests and the exact-head isolated publish evidence recorded by the U4 PACT.
