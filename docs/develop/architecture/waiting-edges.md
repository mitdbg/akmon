---
audience: developer, operator
type: explanation
canonical_id: waiting-edges
section: development
visibility: primary
status: active
owner: architecture
---
# Edges that wait for another source's record

Source B can write an edge whose endpoint another source A owns, such as
`note references widget`, before A has written the widget, after A has
retracted it, or when the record at that id has a subtype B's clause does not
accept. This used to cost B its whole run (`producer_edge_endpoint_missing`)
or stop every publish of the shared branch (the dangling-edge gate). Now the
edge waits. [Code architecture](pipeline.md) lists the publish call path.

**Which edges wait.** An edge waits when it matches one of the producer's edge
clauses with the missing or mismatched endpoint read as a wildcard, and no
matching clause names a subtype the producer emits itself (the clause subtype
and its descendants, read with is-a, must be absent from the producer's own
node signature). A `not_loaded` link-endpoint placeholder never counts as an
existing endpoint. These keep the refusal: a gap in the producer's own
subtype, an undeclared edge type, a clause marked `endpoint_absent: refuse`, a
ranked edge type, and a producer with no edge signature or with unbounded
nodes or edges. The source-sync result reports `deferred_edges`: a count,
counts by state, and a bounded sample of ids.

**The cap.** A wrong id prefix must not park a whole run, so a run that defers
too many edges is refused whole with `producer_deferred_edges_over_limit`.
`consistency.deferred_cap_mode` picks the rule: `owner_published` (default)
counts only deferrals whose endpoint's owning source has published at least
once, up to `consistency.max_deferred_edges_per_run` (default 1000);
`absolute` counts all of them against that number; `fraction` is the smaller
of that number and `consistency.max_deferred_edge_fraction` (default 0.2) of
the run's edges. In `owner_published` mode, deferrals whose owner has never
published are not counted against that number (the ordinary run-order case),
but one run may not park more than
`consistency.max_deferred_edges_unpublished_owner` of them (default 100000).
The default mode is one constant, `DEFAULT_CAP_MODE`.

**Where they wait.** `okg.graph_edge_deferrals` is a Chronos table, so it
branches, merges, checkpoints and rolls back with the graph. A row is the edge
exactly as the producer wrote it, plus the endpoint it waits on, the subtype
the clause expects, `deferred_at`, and a state: `waiting`, `subtype_mismatch`,
`narrowing_violation` or `cardinality_violation`. No read operator serves it,
and the `okg_mcp` read role has no grant on its physical tables. `okg doctor --check deferred-edges` and `okg runtime
deferred-edges` report it by producer, edge type, endpoint subtype and state,
with the oldest age. Only the runtime command lists a bounded id sample (ids
can carry sensitive names); the doctor withholds them and warns, with exit 0,
once the oldest edge passes `consistency.deferred_edge_warn_after` (default 24
hours). The projection remediator is a writer, not a read role: it runs the
writer under its own role, and a retraction withdraws the producer's waiting
rows with its edges, so it holds the same select, insert, update and delete
grants on the physical tables as on every other Chronos table.

**The publish step.** Before the consistency gate, on the full and the
incremental path: (0) a waiting row whose identity is already in
`okg.graph_edges` is deleted; (1) a dangling or producer-mismatched edge whose
clause is deferrable moves to the waiting table; (2) a waiting row whose
endpoints both exist as live, non-placeholder nodes, and which passes the
producer clause with resolved subtypes, moves back into `okg.graph_edges`. A
trial insert is checked by the gate's own narrowing and cardinality queries; a
row that fails stays waiting in the matching held state and never reaches the
gate. A held row is checked again when its endpoint is written, when the
catalog version changes, and when its producer's profile fingerprint changes.
The step commits apart from the checkpoint; a publish retried after a crash
reaches the same graph. A publish whose only change is to the waiting table
cuts a generation: both early no-op probes read the table and the delta counts
carry `deferred_added`, `deferred_retracted` and `deferred_updated`.

**Producers and lifecycle.** A producer's waiting edges are its records. Its
retraction removes them, the writer skips an identical re-emit (so an
unchanged poll cuts no generation), and record-set coverage reads the graph
plus the waiting table. A run-base undo and a source retirement leave another
source's deferrable edge in place instead of deleting it; retiring a source
removes its own waiting edges. Merge-back reads the waiting rows written since
the pre-land generation and puts them back after a rollback; environment clone
purges the table.

**Known race.** The publish step takes the branch write lock only for its own
writes, as the gate's prune does, so a source write that lands between the
step and the checkpoint is picked up by the next publish, not this one.
Owner lookup per publishing source and forgetting rolled-back publishes in
`owner_published` mode are follow-ups.

**Default by deployment.** `consistency.defer_dangling_edges` is `auto`, `true`
or `false`. `auto` defers on `dangling_edges: block` deployments and keeps
`prune` deployments on prune (decision D1). Before changing that default, read
the prune-baseline evidence: a synthetic fixture run and a short sample from one real
deployment that found 3 pruned edges in three weeks.

**Edges already lost to prune.** Run the producers again so the edges are
written and wait: `okg ingest --deployment <name> --include <producers>
--no-predecessors --reset-cursor --inline`, with `<producers>` taken from the
gate result's `pruned_dangling_edge_attribution`. Only a producer with
incremental state (a cursor or watermark) needs `--reset-cursor`; a
whole-scope `reconcile` source re-emits on any run.

Code: `graph_store_internal/edge_deferrals.py`, `producer_authority.py`
(`classify_edge_deferral`), `consistency/transform_gate.py`. Proof:
`tests/substrate/ingest/test_cross_source_endpoint_wedge_db.py`.
