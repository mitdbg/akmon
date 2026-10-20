# Chronos Formal Models

## System

The Chronos model covers OKG read authority over published graph generations.
It focuses on checkpoint publication, session pins, compatibility generation
metadata, graph checkout consistency, and operational SQL read behavior.

## Design Contract

- Safety property: graph and operational SQL reads use an explicit session pin
  or preserved compatibility id; open sessions do not move when newer
  generations publish.
- Authority boundary: immutable substrate Session state in
  `src/okg/substrate/mcp/session.py` and SQL dispatch in
  `src/okg/substrate/mcp/run_sql.py` own read authority.
- Valid runtime paths: pinned graph reads, session-bound SQL reads, rejection of
  missing compatibility metadata, and runtime-refinement coverage of public SDK
  defaults (the transport lifecycle itself remains unmodeled).
- Out of scope: Chronos storage internals, SQL execution semantics, branch
  isolation, and the MCP transport identity/weak-registry lifecycle (including
  first bind, concurrency, refresh, client isolation, stateless behavior, and
  reconnect) remain separate executed integration checks.
- Preconditions/refinement: the public SDK latest-generation fallback and the
  branch-isolation boundary are tracked in `formal/gap-inventory.toml` as
  `chronos.sdk-latest-fallback-unmodeled` and `chronos.branch-isolation-unmodeled`;
  session-pin refinement tests must not be used to claim SDK default-read safety.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `ChronosReadPin.tla` | `ChronosReadPin.cfg` | Session pin and read-side safety contract. The main config keeps the new `CONSTANT BuggyDefaultZeroFallback` set to `FALSE`. |
| `ChronosReadPin.tla` | `ChronosReadPinCounterexample.cfg` | Non-vacuity witness: sets `BuggyDefaultZeroFallback = TRUE` so operational SQL can emit the forbidden `uses_default_zero` outcome, making `OperationalSqlNeverDefaultsGeneration` reachable rather than vacuously true. |
| `ChronosReadPin.tla` | `ChronosReadPinFollowsLatestCounterexample.cfg` | Pin-freeze witness: sets `BuggySessionFollowsLatest = TRUE` so a session's graph read follows `latest` instead of its checkout pin after a publish, refuting `GraphReadUsesPinnedGeneration` by name -- proving the pin-freeze headline is load-bearing, not structurally true. |
| `ChronosRetentionCompaction.tla` | `ChronosRetentionCompaction.cfg` | Tombstone-first history retention, complete plan binding, publisher exclusion, bounded table progress, crash/resume, and replay safety. |
| `ChronosRetentionCompaction.tla` | `ChronosRetentionCompactionProtectionCounterexample.cfg` | Protection witness: ignoring the protected floor can delete a current or pinned read. |
| `ChronosRetentionCompaction.tla` | `ChronosRetentionCompactionLockCounterexample.cfg` | Publisher-overlap witness: removing mutual exclusion permits publication during destructive compaction. |
| `ChronosRetentionCompaction.tla` | `ChronosRetentionCompactionStalePlanCounterexample.cfg` | Stale-plan witness: mutating after a bound input drifts refutes plan freshness. |
| `ChronosRetentionCompaction.tla` | `ChronosRetentionCompactionReadInterleavingCounterexample.cfg` | Unleased-reader witness: allowing an already-open historical reader without a protection lease lets compaction invalidate that read. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `ChronosReadPin.tla` | A published generation is opened as a read session, and later publishes do not move that session. | `src/okg/substrate/mcp/session.py`, `src/okg/substrate/mcp/operators.py` | `tests/substrate/test_chronos_graph_store.py`, `tests/substrate/test_mcp_tools.py` |
| `ChronosReadPin.tla` | Graph and SQL reads choose the session pin, not the newest generation or a default generation. | `src/okg/substrate/mcp/run_sql.py`, `src/okg/deployment/_internal/reads.py` | `tests/substrate/mcp/test_run_sql_dispatcher.py`, `tests/substrate/test_chronos_graph_store.py` |
| Runtime integration (not modeled by `ChronosReadPin.tla`) | A stable MCP transport identity binds and reuses one default generation; explicit overrides, refresh, weak lifecycle, concurrency, client isolation, stateless honesty, and connection replacement follow the public contract. | `src/okg/substrate/mcp/logical_session.py`, `src/okg/substrate/mcp/server.py` | `tests/substrate/mcp/test_logical_session_generation_pin.py` |
| `ChronosRetentionCompaction.tla` | A confirmed retention plan tombstones incomplete checkpoints before bounded deletion, excludes concurrent publication, protects current/fork/pin/lease reads, and resumes idempotently after interruption. | `src/okg/substrate/history_compaction.py`, `src/okg/substrate/history_read_guard.py`, `src/okg/substrate/cli/generation.py` | `tests/substrate/test_chronos_compaction.py` |

## Invariant Map

| Invariants | What it means | Code/test signal |
| --- | --- | --- |
| `TypeOK` | Read-pin sessions, generations, compatibility ids, and observations—and compaction phases, protection sets, plan bindings, locks, progress, tombstones, refusal, and replay—keep their expected shape. | Session construction in `src/okg/substrate/mcp/session.py`, compaction state in `src/okg/substrate/history_compaction.py`, `tests/substrate/test_mcp_tools.py`, and `tests/substrate/test_chronos_compaction.py`. |
| `PinnedOnlyPublishedOkgGeneration`, `NoUnpinnedRead` | A session can only read a published OKG generation, and reads without a pin are rejected. | Open-session tests in `tests/substrate/test_mcp_tools.py`. |
| `SessionPinFrozenAcrossPublish`, `LatestAdvanceDoesNotMutateOpenSession` | Publishing a newer generation must not move an existing session. | `tests/substrate/test_chronos_graph_store.py`. |
| `GraphReadUsesPinnedGeneration`, `NoMixedGenerationObservation` | Graph reads must come from one pinned generation, not a mix of old and new rows. | `tests/substrate/test_chronos_graph_store.py` and `tests/substrate/mcp/test_run_sql_dispatcher.py`. |
| `SessionPreservesCheckpointCompat`, `OperationalSqlNeverDefaultsGeneration`, `OperationalSqlRequiresSessionCompatOrRejects`, `OperationalSqlUsesOnlyPreservedCompat`, `OperationalSqlRejectsMissingSessionCompat` | Operational SQL must use the compatibility id captured by the session, or reject when that id is unavailable. `OperationalSqlNeverDefaultsGeneration` is a reachable-state invariant with a non-vacuity witness: `ChronosReadPinCounterexample.cfg` flips `BuggyDefaultZeroFallback` to `TRUE` so the forbidden `int(0)`-fallback outcome is reachable and TLC refutes the invariant, proving the main-config invariant is load-bearing. | `src/okg/substrate/mcp/run_sql.py`, `src/okg/deployment/_internal/reads.py`, and `tests/substrate/mcp/test_run_sql_dispatcher.py`. `ChronosReadPinCounterexample.cfg` must report `OperationalSqlNeverDefaultsGeneration` violated. |
| `ChronosReadPinSafety` | The session pin contract holds across graph reads, SQL reads, and later publishes. | Combined session/read tests in `tests/substrate/test_chronos_graph_store.py`. |
| `CurrentAndProtectedReadsPreserved`, `FloorNeverCrossesProtection`, `EveryActiveReaderHasLease` | Compaction never crosses the current, audit-tail, fork, named-pin, or active-reader protection floor, and every historical reader holds a lease while its checkout is exposed. | Protection-set and read-lease cases in `tests/substrate/test_chronos_compaction.py`. |
| `PublishAndCompactionMutuallyExclusive`, `PlanBindingsFreshWhileMutating` | Publication cannot overlap compaction, and any drift in policy, head, checkpoints, catalog, table mapping, or protection invalidates mutation. | Advisory-lock and binding-drift cases in `tests/substrate/test_chronos_compaction.py`. |
| `TableBatchesAreAtomicAndOrdered`, `CrashStateIsValidOrResumable`, `ReplayIsIdempotent` | Each physical-table batch commits atomically, crash state always identifies the next safe batch, and replay cannot delete additional protected state. | Tombstone-first resume/replay cases in `tests/substrate/test_chronos_compaction.py`. |
| `HistoricalReadsAreCompleteOrRefused`, `RetiredGenerationRefusalIsStable` | A historical checkout is either complete or refused by a durable tombstone before deletion begins; retries return the same refusal. | `src/okg/substrate/history_read_guard.py` and compacted-generation read tests in `tests/substrate/test_chronos_compaction.py`. |
| `RetentionCompactionSafety` | A confirmed plan mutates only while every binding is current; checkpoints refuse before row deletion; current/fork/pin/lease reads survive; each table batch resumes atomically. | `src/okg/substrate/history_compaction.py`, `tests/substrate/test_chronos_compaction.py`, and `scripts/prove_chronos_history_compaction.py`. |

## Verification Process

Run:

```sh
uv run --extra dev python scripts/check_formal_chronos_read_pin.py \
  --out .scratch/formal/chronos-read-pin

uv run --extra dev python scripts/check_formal_chronos_retention_compaction.py \
  --out .scratch/formal/chronos-retention-compaction
```

The checker emits a strict formal report and fails if TLC, Java, artifacts, or
property rows are missing. Change records should attach that report, not a
hand-written summary.

## Model Boundary

Implementation tests must exercise the MCP session lifecycle, graph SQL checkout
selection, default-session query path, and operational SQL rejection behavior.
The model does not prove Chronos storage internals, SQL execution semantics, or
MCP transport identity/lifecycle behavior; it proves only that an already-open
substrate Session remains pinned across reads and later publishes. The public
MCP lifecycle claims require the connected transport/publish tests named above.
