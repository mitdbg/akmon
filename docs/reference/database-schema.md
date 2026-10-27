---
audience: developer, operator
type: reference
canonical_id: database-schema
section: reference
visibility: primary
status: active
owner: database
---
# Database schema

AKMON stores each deployment in PostgreSQL. This page names the stable parts
of that database and the files that define them.

## Source of truth

| Database concern | Authority |
|---|---|
| AKMON tables and functions | `src/okg/substrate/db/schema.sql` and the owning package's schema module |
| Read views and grants | `src/okg/substrate/db/views.sql` |
| Ordered database changes | `src/okg/substrate/db/migrations/manifest.yaml` and the SQL files it lists |
| Graph storage | `src/okg/substrate/graph_store_internal/` and the vendored Chronos package |
| Durable workflow state | DBOS's own schema |

Run database changes through `okg migrate`. Do not apply a migration file by
itself: the manifest defines order, prerequisites, checksums, and whether a
change needs explicit approval.

## Schema design rules

1. Public graph versions use text references. Integer ordinals are internal
   ordering aids, not public identity.
2. `branch_id` identifies an AKMON branch. `chronos_branch_id` identifies its
   storage-layer counterpart. Code must not treat them as interchangeable.
3. `deployment_name` identifies the deployment that owns a row when data can
   share a database.
4. Readers use approved views or component APIs, not private storage tables.
5. Derived indexes and caches are rebuildable. An empty derived relation does
   not prove that the source is empty.
6. Schema identity, namespace placement, migrations, and grants are checked
   against the live database rather than inferred from a successful startup.

### Namespace contract

AKMON-owned relations live in `okg`; derived read structures live in
`okg_derived`; DBOS owns its workflow schema. Vendored Chronos relations and
the small set of intentional `public` exceptions are checked by
`scripts/check_schema_namespace.py`. Deliberate identity exceptions are listed
in `src/okg/substrate/db/identity_divergences.yaml`.

## Relation families

The table names below are landmarks. Feature-specific relations stay defined
by the owning schema file; inspect the PostgreSQL catalog when exact columns or
constraints matter.

| Family | Representative relations | What they hold |
|---|---|---|
| Graph and versions | `okg.graph_nodes`, `okg.graph_edges`, `okg.graph_generations`, `okg.branches`, `okg.branch_merge_requests` | Versioned records, relationships, branches, and publication state |
| Catalog | `okg.catalog_versions`, `okg.subtypes`, `okg.subtype_ancestors`, `okg.narrowings`, `okg.aliases`, `okg.identifier_patterns`, `okg.invariants` | The types and rules used to interpret and validate a graph |
| Source facts | `okg.source_watermarks`, `okg.source_record_sets`, `okg.node_facts`, `okg.edge_facts`, `okg.fact_batch_receipts`, `okg.source_retirement_receipts` | Imported source state and normalized facts before publication |
| Admission | `okg.producer_profiles`, `okg.admission_envelopes`, `okg.admission_events`, `okg.admission_record_refs`, `okg.envelope_violations` | Who submitted a change, what was accepted, and why a record was rejected |
| Access decisions | `okg.nomos_policy_bundles`, `okg.nomos_policy_activations`, `okg.nomos_identity_providers`, `okg.nomos_subject_bindings`, `okg.nomos_permission_grants`, `okg.nomos_decisions` | Policy, identity, permissions, and decision evidence |
| Search and attributes | `okg.attribute_observations`, `okg.attribute_effective_attrs`, `okg.declared_attr_indexes`, `okg.generation_ordinals` | Typed attribute history and the declared attribute indexes |
| Consistency checks | `okg.consistency_check_definitions`, `okg.consistency_candidates`, `okg.consistency_candidate_coverage`, `okg.consistency_gate_results` | Candidate comparisons and publication check results |
| Vector sets | [Below](#vector-sets) | Semantic search vectors |
| Blob store | `okg.blobs` (kind `image` only), read by `okg_mcp` through `okg.image_blobs` | Bytes the graph names by id, or a named render error; `okg db prune-blobs --kind image` deletes rows no retained generation references, after a grace window |
| Operations and telemetry | `okg.source_progress`, `okg.telemetry_traces`, `okg.telemetry_spans`, `okg.telemetry_events`, `okg.telemetry_metrics`, `okg.telemetry_artifacts` | Import progress and operational evidence |
| Schema and grants | `okg.schema_migrations`, `okg.schema_migration_phases`, `okg.grant_registry`, `okg.pending_grants` | Applied changes and access-control drift |
| PACT coordination | `okg.pact_decisions`, `okg.pact_intents`, `okg.pact_coordination_events` | Durable work decisions and coordination events |

## Views and read surfaces

Use views when reading published state. Important groups:

- graph: `okg.v_nodes`, `okg.v_edges`, `okg.v_generations`;
- documents: `okg.v_documents`, `okg.v_doc_chunks`,
  `okg.v_chunk_sequence`, `okg.v_table_cells`;
- sources and admission: `okg.v_source_runs`,
  `okg.v_source_run_diagnostics`, `okg.v_admission_envelopes`,
  `okg.v_admission_events`;
- access decisions: the `okg.v_nomos_*` views;
- operations: `okg.v_runtime_queue_health`,
  `okg.v_runtime_stale_locks`, `okg.v_compute_host_metrics`;
- MCP and program execution: `okg.v_mcp_calls`,
  `okg.v_mcp_operator_costs`, `okg.v_ir_programs`,
  `okg.v_ir_invocations`.

`v_runtime_queue_health` emits an explicit unavailable row when DBOS state is
missing. Missing workflow state must not look like a healthy empty queue.

The [MCP views reference](mcp.md) describes the stable
graph-read views available to MCP clients.

## Attribute filter index

MCP `filter`, `map`, and `search(method=attrs)` narrow an attribute filter
only through the declared node expression indexes `okg db attr-index
reconcile` builds (`OKG_ATTR_FILTER_INDEX`, see the CLI reference); any other
filter checks every record and notes `"not_covered"`. Migration
`0043_attr_index_teardown` drops the former typed side
table `okg.attr_index`, its coverage markers, rebuild queue and derive front;
see the CLI reference for the upgrade order.

## Vector sets

| Table | Key | Holds |
|---|---|---|
| `okg.embedding_sets` | `set_id` (`es_` + 24 hex) | Identity, width, templates, index, `active`, link counter |
| `okg.embedding_vector_counters` | `set_id` | Vector counter |
| `okg.text_embeddings` | `(set_id, doc_hash)` | A `halfvec` (`embedding`) or `vector` (`embedding_f32`) and `seq` per document |
| `okg.node_documents` | `(set_id, template_hash, node_id, row_hash, seq)` | Node content's document |
| `okg.embedding_templates` | `(set_id, template_hash)` | Template sources |
| `okg.embedding_watermarks` | `(generation_id, set_id)` | A generation's `seq` bounds, fill state |
| `okg.embedding_fill_completions` | `(set_id, generation_id)` | Fills that left nothing missing |

`app_rw` may SELECT and INSERT the last five and update the counters;
`okg_mcp` reads through grant-registry rows; `app_ro` gets nothing. A
`hnsw` set's partial index is `text_embeddings_<set hex>_<build hash>_hnsw`.

## Schema and grant checks

Use the supported commands instead of comparing this page with the database by
hand:

```bash
uv run okg migrate --deployment <deployment>
uv run okg schema quality --dsn <freshly-migrated-test-database>
uv run okg doctor --deployment <deployment> --check grant-registry-drift
uv run okg doctor --deployment <deployment> --check pending-grants
```

`okg schema quality` scores the schema this checkout defines, not the
deployment's database. Point `--dsn` at a scratch database migrated from the
same checkout; without it, only the static checks run.

Stop on a failed migration, schema-quality check, or grant check. Inspect the
live catalog and the named authority file before changing application code or
marking the deployment ready.

## Scope

This reference covers AKMON-owned database structures. PostgreSQL system
catalogs, extensions, DBOS internals, and Chronos physical tables follow their
own contracts.
