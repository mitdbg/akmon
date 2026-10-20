---
audience: developer
type: reference
canonical_id: responsibility-map
section: development
visibility: primary
status: active
owner: architecture
---
# Responsibility map

Find the owner of a behavior before changing it: the module to edit, the tests that pin it, and whether a database is required.

[Code architecture](pipeline.md) orders the same packages by call path;
[System architecture](overview.md) defines what each boundary is allowed to do.
This page is the current map; it does not record earlier inventories,
slices or ordering decisions.

**Pure rules** means code that computes a decision from its inputs with no
database, file or network access. A pure owner can be changed and tested
without setting up PostgreSQL, which is why the columns separate it from the
loading, execution and persistence around it.

The columns read left to right as a maintenance path: the public name callers
depend on, the decision logic, the code that fetches and writes, the symbols to
grep for, the focused tests, and the setup cost of running them. Paths are
under `src/okg/substrate/` unless stated otherwise.

The database column and the symbol names are a snapshot at commit
`df0ad8b8a8`, read from each file's `pytest.mark.postgres` marks rather than
from any rule that is enforced. A test can gain or lose a mark after this page
was written, so re-check a cell before relying on it:
`pytest -m "not postgres" <file>` collects only the part that needs no
database, and its count tells you whether the cell still holds.

## Where each behavior lives

| Task | Public entry | Pure rules (no I/O) | Load / execute / persist | Key symbols | Tests that pin it | Database needed for the focused test |
|---|---|---|---|---|---|---|
| A. Change an external connector's run, context or signal behavior | `src/okg/deployment/__init__.py` (`__all__`) | `src/okg/deployment/_internal/protocol.py` | `ConnectorAdapter.run`; runner argument selection in `ingest/runner.py`; `src/okg/deployment/_internal/registration.py` | `ConnectorAdapter`, `_adapter_run_kwargs` | `tests/substrate/runtime/test_connector_protocol.py`, `test_connector_registration.py`, `test_connector_conformance.py` | No — none of the 65 tests is marked |
| B. Change ingest source selection, order or cycle rules | `ingest/service.py` `run_ingest` | `ingest/planning.py` | `ingest/service.py` `_run_ingest_inner`; `ingest/runner.py` | `plan_ingest_sources`, `topo_layers`, `resolve_predecessor_deps` | `tests/substrate/ingest/test_planning_boundary.py`, `tests/architecture/test_ingest_planning_boundaries.py`; `tests/substrate/test_ingest_service.py` | No for the two boundary tests (0 of 7 and 0 of 17); 53 of the service test's 98 tests need one |
| C. Change derived-artifact scheduler dependency validation, order or hash | `derived_artifacts/__init__.py` re-exports | None yet — the rules are still inline in `derived_artifacts/scheduler.py` | `scheduler.py` `_load_dependencies`, `_create_plan`; real caller `publisher/native_view_stage.py` | `_require_known_dependencies`, `_topological_order`, `_graph_hash`; `stable_hash` in `derived_artifacts/relations.py` | `tests/substrate/test_derived_artifact_scheduler.py`, `test_lineage_driven_scheduler.py`, `test_derived_artifacts.py` | Yes — all three files are marked at file level, so every test needs one. This is what Slice 5 changes |
| D. Change a publication check | `publisher/publisher.py` `Publisher.cycle` | None — `publisher/gates.py` is a separate module but runs SQL | `publisher/gates.py` | `evaluate_gates`, `preflight_invariants` | `tests/substrate/test_gates.py`, `tests/substrate/publisher/test_invariant_gate_pinned_read.py`, `tests/substrate/test_invariants_loader.py` | Yes for the two gate tests; the invariants-loader test needs one for only 3 of its 27 tests |
| E. Change a relationship builder (enricher) or linker | `publisher/extractor_stage.py` `ChronosExtractorStage`; `Publisher._run_enrichers` | The matching rules inside each linker under `library/linkers/` and each enricher such as `enrichers/derived_edges.py`; protocol in `enrichers/base.py` | `publisher/enricher_loader.py` | `Enricher`, `load_enrichers`, `derive_write_envelope`, `validate_enricher_envelopes` | `tests/substrate/test_linker_router_units.py`; `tests/substrate/enrichers/` including `test_derived_edges.py` | Mixed — none of the 13 linker unit tests; all 5 derived-edge tests |
| F. Change an ontology module or allowed-relationship rule | `catalog/modules_compose.py` `compose_catalog` | Validation inline in the composer; the file opens no database connection | Runtime enforcement in `catalog/narrowings.py` | `_validate_narrowing_subtypes`, `_check_no_contains_cycles`, `_extract_narrowings`; `NarrowingChecker`, `assert_narrowing_permitted` | `tests/substrate/catalog/test_module_manifests.py`, `test_semantic_catalog_composition.py`; `tests/substrate/test_narrowings.py` | Mixed — none of the 17 manifest tests; 3 of the 10 composition tests connect; all 4 narrowing tests |
| G. Change a graph read exposed through the agent tool connection (MCP) | `mcp/server.py`, with `mcp/tool_registration.py` and `mcp/tools.py` | `mcp/operator_sql.py` | `mcp/server.py`; descriptions and classification in `mcp/operators.py` | `predicate_sql`, `projection_sql`, `field_expr`; `substrate_tool_description`, `classify_tool` | `tests/substrate/test_mcp_server.py`; `tests/substrate/mcp/` | Yes — the server test is marked at file level, so all 21 of its tests need one |
| H. Change a CLI command family | `cli/main.py` | None | `cli/main.py`, which still defines 28 `_cmd_*` handlers inline; parsers in `cli/parser_groups.py`; 17 extracted family modules imported at the top of `main.py` | `_cmd_migrate`, `_cmd_run`, `_cmd_ingest`, `_cmd_sync`, `_cmd_status` and 23 more | `tests/substrate/cli/test_cli_smoke.py`; the ten `tests/substrate/cli/test_cli_*_module_location.py` files pin the extraction boundary | No — none of the smoke test's 3 tests is marked |
| I. Change a durable workflow or schedule | `runtime/worker.py` registration | None — the policy is inline | `runtime/source_sync_workflow.py`, `publisher_workflow.py`, `publisher_schedule.py`, `source_poll_schedule.py`, `_schedule_factory.py`, `scheduled.py` | `register_publisher_workflow`, `register_publisher_schedule` | `tests/substrate/runtime/test_source_sync_workflow.py`, `test_publisher_workflow.py`, `test_schedule_overlap.py`, `test_worker_schedule_modes.py` | Mixed — one test in each workflow file needs a database (1 of 14, 1 of 21); the two schedule tests need none (0 of 7, 0 of 49) |
| J. Change a database migration | `db/migrate.py` | None | `db/migrate.py` with `db/schema.sql`, `db/views.sql` and 37 migration files under `db/migrations/` | `MigrateResult`, `ensure_required_extensions` | `tests/substrate/test_migrate.py` | Yes — marked at file level, so all 27 tests need one |

### Called from

The caller matters as much as the owner: it is what a behavior change has to
keep working. One main in-repo caller per row, at `df0ad8b8a8`.

- **A** — `distributions/archi_profile.py:401` subclasses `ConnectorAdapter`;
  `ingest/runner.py:3346` selects its arguments.
- **B** — `runtime/source_sync_workflow.py:518` calls `run_ingest`.
- **C** — `publisher/native_view_stage.py:966` calls
  `schedule_derived_artifacts`, with skipped registration and coalesced
  transactions.
- **D** — `runtime/publisher_workflow.py:97` runs one publisher cycle;
  `merge_back.py:193` runs another.
- **E** — `publisher/publisher.py:1478` builds the extractor stage through
  `ChronosExtractorStage.from_config`.
- **F** — `deployment_lint.py:2573`, `deployment_release.py:224` and
  `sealed_release.py:679` call `compose_catalog`.
- **G** — `mcp/server.py:3743` imports the SQL helpers from `operator_sql.py`.
- **H** — the `okg` console script, `okg.substrate.cli.main:main`.
- **I** — `cli/runtime.py:206` calls `run_worker`.
- **J** — `cli/provision.py:702` calls `ensure_required_extensions`;
  `cli/init.py:983` imports the same module.

## Extraction status

Line counts are from `origin/dev` at `df0ad8b8a8`; they date quickly, so
recount before quoting one.

**Pure rules already separate.** Row A — `_internal/protocol.py` is 467 lines
behind a 354-line public surface. Row B — `ingest/planning.py` is 381 lines,
extracted from `ingest/service.py`, now 4,130. Row G — `mcp/operator_sql.py`
is 205 lines and imports only `json` and `typing`, alongside a 9,469-line
`mcp/server.py`.

**Separate module, but bound to the database.** Row D — `publisher/gates.py`
is 710 lines split out of a 13,592-line `publisher/publisher.py`, but it
imports `psycopg` and issues its own queries, so it is not testable without
one. Row E — `publisher/enricher_loader.py` is 691 lines and the `Enricher`
protocol is 211; the rules themselves sit in 21 linker modules and in
enrichers such as the 382-line `derived_edges.py`.

**Still inline.** Row C — `derived_artifacts/scheduler.py` is 2,542 lines and
interleaves the three pure functions with loading and plan writes. Row F —
`catalog/modules_compose.py` is 1,545 lines; its validation is pure but has no
separate owner, while `catalog/narrowings.py` (247 lines) enforces the same
rules against a live database. Row H — `cli/main.py` is 11,222 lines after 17
families moved out. Row I — `runtime/worker.py` is 2,425 lines with schedule
policy inline. Row J — `db/migrate.py` is 1,871 lines over a 16,531-line
`schema.sql`.

## Modularization order

Four slices are complete, each landed from current `dev` with its own review:
ingest planning,
source-sync status,
the source-sync optimization ledger, and the GraphCanvas
algorithms.

Slice 5 is scheduler
dependency planning, row C. It is deliberately narrower than the historical
"derived scheduler store" item, which stays deferred — its plan, binding and
transition persistence proposal still needs explicit transaction and caller
evidence before anyone opens it.

After Slice 5 lands, the measured result decides what follows. If moving the
scheduler's dependency rules into a pure owner makes that maintenance task
meaningfully cheaper, the next candidate is the catalog composer's validation
(row F); if it does not, the scheduler store returns to the front. A smaller
file is not the measurement; task locality and setup effort are.

Four rules hold for every slice: one code-bearing slice at a time; publisher
decomposition stays with its own owner and is never folded in; each slice
starts from current `dev` with an independent review; and each adds a seeded
regrowth guard so the boundary cannot quietly close again.

Earlier written orderings are history. They record what
was proposed and why, not what happens next.
