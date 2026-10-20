---
audience: developer, operator
type: explanation
canonical_id: ingest-publish-pipeline
section: development
visibility: primary
status: active
owner: architecture
---
# Import and publication pipeline

Use this map after identifying which component owns a change; it locates the
current calls and packages. [How information becomes
searchable](../../start/data-lifecycle.md) explains user-visible behavior;
[System architecture](overview.md) defines component contracts, authority,
fallbacks, and limits. The
[Responsibility map](responsibility-map.md) lists the same packages by
maintenance task instead of by call order, with the tests that pin each one
and which of them need a database.

## Call path

The names below are the current implementation path for one source-to-read
cycle, ordered by invocation, not by ownership or deployment topology.

| Call step | Current entry point | Main package and symbols |
|---|---|---|
| Resolve a deployment and its sources | Deployment command or runtime workflow | `src/okg/substrate/deployments/`; deployment manifest and source registry |
| Read a bounded source | Adapter runner invokes the selected adapter | `src/okg/substrate/library/sources/base.py`: `SourceAdapter`, `SourceRun`, `NodeFact`, `EdgeFact` |
| Validate and apply a source run | `SourceRunner` → batch application | `src/okg/substrate/ingest/runner.py`: `SourceRunner`; `src/okg/substrate/fact_batch.py`: `FactBatch`; `src/okg/substrate/fact_application.py`: `FactBatchApplication` |
| Update the working branch | Fact application calls the graph store | `src/okg/substrate/graph_store.py`: `ChronosGraphStore.apply_facts` |
| Run configured derivations | Publisher calls extractor and relationship code against a fixed view | `src/okg/substrate/publisher/extractor_stage.py`; `src/okg/substrate/projection/`; `src/okg/substrate/enrichers/`; `src/okg/substrate/library/linkers/` |
| Check and publish a candidate | Publisher cycle invokes gates and checkpoint/pointer writes | `src/okg/substrate/publisher/publisher.py`: `Publisher.cycle()`; publication stack under `src/okg/substrate/publisher/` |
| Serve a graph read | CLI or MCP operator resolves the read surface | `src/okg/substrate/mcp/`: bounded read operators and recipes; commands such as `okg search` and `okg trace` |
| Schedule and recover the calls above | DBOS worker dispatches workflow wrappers | `src/okg/substrate/runtime/`: `source_sync_workflow`, `publisher_workflow`, schedules, worker |

### Extractor and derived facts

During publication, the publisher runs separate derivation phases against the
fixed candidate view. `ChronosExtractorStage` runs the configured extractor and
routing/linking work; `Publisher._run_enrichers` later runs configured
enrichers. Their output returns to the normal fact-application path with its
derivation evidence; none of them writes a published checkpoint directly.

## Publication result types

`PhaseAResult` and `PhaseBResult` are internal Python results used by the
publisher and gate evaluator. Their names are compatibility identifiers; AKMON
does not expose separate public Phase A or Phase B services.

`PhaseAResult` reports whether source-side preparation converged before checks
run:

| Field | Shape and rule |
| --- | --- |
| `converged` | `bool`; must be false when `timed_out` is true |
| `per_source_status` | source to `DONE`, `FAILED`, `TIMED_OUT`, or `NO_WORK` |
| `projection_cursor_by_source` | compatibility cursor details; may be empty for a Chronos-native cycle |
| `timed_out` | whether preparation exceeded its deadline |
| `delta` | `ProjectionDelta` counts for nodes and edges added or retracted |

`GateInvariantResult` describes one configured check: `name`, source or
cross-source `scope`, optional `source`, `severity`, `passed`, bounded
`violators`, total `violator_count`, `duration_ms`, `timed_out`, and optional
`metadata`.

`PhaseBResult` contains `source_local` and `cross_source` check results,
`excluded_sources`, `blocked`, structured `blocked_reason`, total duration and
budget status, plus optional cache, summary, consistency, and substrate
integrity results. `GateResult` is a compatibility alias for the same Python
type. When `blocked` is true, publication must leave the previous generation
active and record the blocking reason.

Each violator row is the JSON object returned by an invariant query. It must
identify at least one `node_id` or `edge_id`; other context columns are
preserved. A row with neither identifier fails with
`invariant_sample_unidentifiable`, because it cannot be traced or audited.

## Extension-to-call-path map

| Change | Register or extend at | Next call in the path |
|---|---|---|
| Source format or service | `SourceAdapter` and the source registry | `SourceRunner` → `FactBatchApplication` |
| Derived relationship | Extractor, linker, or enricher registration | Publisher extractor stage → normal fact application |
| Publication rule | Catalog invariant or publication gate | `Publisher.cycle()` → gate evaluation |
| Read operation | Existing MCP operator, view, or bounded recipe | Read authority and generation-resolution path |
| Durable trigger or retry | DBOS workflow/schedule wrapper | The owning source, publisher, or read operation |

Poros exchange, Nomos policy, and federation cross this path without replacing
its calls. Their boundaries are in
[Exchange between deployments](overview.md#exchange-between-deployments).

## Rows several producers write

`okg.graph_edges` keeps one row per (source node, edge type, target node).
When several producers write the same edge, as `code-structure`,
`code-lsp-resolution` and the `code_reference` enricher do for `calls` and
`references`, the row used to belong to whichever wrote last, so an
incremental publish and a rebuild could store different rows. A deployment
now ranks those producers, highest first:

```yaml
edge_precedence:
  calls: [_enricher:code_reference, code-lsp-resolution, code-structure]
  references: [_enricher:code_reference, code-lsp-resolution, code-structure]
```

For a listed type, `okg.graph_edge_declarers` (a Chronos table, so it
branches, merges, checkpoints and is undone with the graph) keeps each listed
producer's current version of an edge, and the graph stores the version of the
highest-ranked producer that still declares it. A retraction passes the row to
the next-ranked declarer or deletes it when none remains; a deleted node takes
every declaration of its ranked edges with it. Unlisted types and unlisted
producers keep last-writer-wins. Lint refuses a ranked producer sharing a
declared triple with an unlisted one, and the writer refuses an unlisted
producer writing an edge a ranked producer declares. The writer reads the
ranking from `okg.edge_precedence`, which `okg migrate --deployment <name>
--apply` and the worker at start copy from `deployment.yaml`; a listed
source-registry name also ranks that source's `ownership_id`, the identity
ingest writes under. The `code_reference` enricher compares a derived edge with
its own declaration and retracts declarations it no longer derives within what
its run read. Code: `graph_store_internal/edge_declarers.py`; proof:
`tests/substrate/test_edge_declarers_db.py`.

A ranked producer's stored row from before the ranking is adopted as its
declaration when the edge is next written, so turning a ranking on loses no row;
an unlisted or unattributed writer's row is not adopted. An overwritten version
returns only when its producer writes the edge again. For a Workspace Graph
deployment such as `my-graph`, after `okg migrate --deployment my-graph --apply`
and a worker restart, re-run the ranked sources with the enricher forced to a
full run (leave out a disabled source):

```bash
okg ingest --deployment my-graph \
  --include code-structure,code-lsp-resolution --no-predecessors \
  --reset-cursor --inline --force-enrichers
```

The forced enricher run re-declares every edge it derives, outranking the
adopted row. A ranking change needs the same runs. `okg migrate --deployment`
and worker start copy the ranking and remove the declarations it does not cover
in one transaction, on every run and branch holding any, so a re-run also
removes stray ones. They hold the ranking lock exclusively, with no timeout: a
long ranked write delays them, and new ranked writes queue behind them. Every
path that writes ranked edges or declarations holds it shared, so during `okg
migrate` writes of ranked edges and edge or node deletes wait until migrate
commits. A write planned with an old ranking is refused before writing
(`edge_ranking_changed_during_write`); in a caller's transaction the whole
transaction rolls back, so rerun it. Paths that take a branch lock first reverse
the lock order, so the change yields: it waits a quarter of `deadlock_timeout`
per branch lock, then refuses (`edge_ranking_change_busy`) with nothing changed.
On a busy deployment where some branch holds uncovered declarations, a change or
stray-row repair may be refused repeatedly (worker start exits 2 until it wins);
retry when quiet. A frozen paired workspace accepts no write: one without stale
declarations is skipped with a warning; one holding them refuses the change
until the old ranking is restored or the branch is deleted (`okg branch delete
<branch> --deployment <name> --confirm`, `--cascade` also deleting child
branches); under `okg migrate` that fails the whole migrate, unrelated
migrations included. A merge stores the winner of each ranked edge whose
declarations it changed and keeps a row no ranked producer wrote; a merge into a
frozen paired workspace is refused. A merge-back rollback restores declarations
written since the last published generation onto live nodes.

Nodes follow a matching rule without a ranking. A node whose id comes from
content can be emitted by several sources: a git-history `author:` node in every
repository an author touched, or a doc-corpus chunk, `visual:` or `mention:`
node whose text or image appears in two repositories. Such a source declares the
node `ownership_mode="substitute"`. The graph stores one row and keeps its owner
(its first writer, or on a deployment upgraded from authoritative chunks its
last writer before the upgrade); every emitting source records a claim in
`okg.graph_node_declarers`; record-set coverage accepts the node under any
owner; and a retraction, a compensation without a run base, or a run-base undo
by the owner passes the row to the surviving claimant with the lowest source
name instead of deleting it. Which claimant's copy the row holds can depend on
commit order; the set of nodes, edges and attributes does not. An edge whose two
endpoints are both shared nodes has no claim table yet: the owner's retraction
still deletes it, and the other source's next full coverage check then fails on
the missing edge. Code: `graph_store_internal/node_declarers.py`,
`ingest/compensation.py`; proof:
`tests/substrate/library/sources/test_doc_corpus_shared_chunks_db.py`.
