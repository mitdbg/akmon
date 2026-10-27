---
audience: operator
type: how-to
canonical_id: runbook-host-storage-reclaim
---
# Diagnose and reclaim storage

Use the branch matching the failing layer:

- graph reads disagree with a publication or Chronos permissions fail: verify
  versioned graph storage;
- interval history grows while the current graph stays small: compact graph
  history;
- the host-headroom check crosses its configured floor: reclaim host storage.

Do not use host deletion to repair graph history, or grant a general read role
access to Chronos physical tables.

## Verify versioned graph storage

Published generations are Chronos checkpoints; imports write through a mutable
branch, while pinned readers use `okg.v_nodes` and `okg.v_edges`. Catalog,
source, policy, and audit state remain ordinary OKG tables.

```bash
uv run okg generation list --deployment <deployment>
uv run okg doctor --deployment <deployment>
uv run okg run --once --deployment <deployment> --apply --json
```

If MCP reads fail with `permission denied for _chronos_branch_tables` or on
another `_chronos_branch_*` relation, refresh schema and view grants, then
verify readiness:

```bash
uv run okg migrate --deployment <deployment> --apply --views-only
uv run okg deployment ready <deployment> --json
```

Omit `--views-only` when the deployment has not run current migrations. The
least-privilege `okg_mcp` role owns Chronos metadata access and membership in
`app_ro`. Do not grant `app_ro` direct access to physical Chronos tables. Stop
the rollout if generation IDs are not Chronos checkpoints. After repair,
compare pinned counts through `okg.v_nodes` and `okg.v_edges`, restart twice to
prove idempotence, and confirm all readers still pin the latest checkpoint.

## Compact graph history

`chronos.history_retention` sets retention; the scaffold ships
`default: {horizon: 14d}`, `audit_tail_generations: 50`. `latest-only` keeps
current state plus the audit tail; `full` keeps all. Unknown
classifiers, fork bases, pins, leases, or ordinals block compaction. Declaring
it turns off the publisher prune, so compact on a schedule; undeclared,
compaction uses that default, printing `policy_source: built_in_default`.

Dry-run creates an immutable plan:

```bash
uv run okg generation compact --deployment <deployment> --json
```

Review its policy digest, checkpoint vector, tables, catalog, protection set,
eligible rows and bytes, and downtime posture, then apply it:

```bash
uv run okg generation compact --deployment <deployment> \
  --confirm <plan-id> --apply --json
```

Checkpoint tombstones commit before deletion, so retired checkouts fail with
`generation_compacted` rather than return a partial graph. Batches record
durable progress; after an interruption, replay the confirmed plan. Any
policy, head, checkpoint, catalog, table-map, or protection drift needs a new
dry-run.

Logical deletion reuses pages but may not shrink the filesystem. Before
changing retention or publish cadence, measure interval rows, relation and
index bytes, WAL per generation, publish p95, and temporary reclaim headroom.
For publish cadence, there is no universal safe interval. Inspect physical size with:

```sql
SELECT relname, pg_total_relation_size(relid), n_live_tup, n_dead_tup
FROM pg_stat_user_tables
WHERE relname LIKE '_chronos_b_interval_%';
```

Physical shrink is offline only: stop publisher and source-sync workers, verify
headroom for the largest relation and its indexes, then run:

```bash
uv run okg generation compact --deployment <deployment> \
  --confirm <plan-id> --apply \
  --offline-reclaim --workers-stopped --json
```

This guarded path may use `VACUUM FULL`; live `VACUUM FULL` is forbidden. Verify protected
checkouts, row deltas, generation status, queues, DLQ, and doctor. On failure,
keep the plan and tombstones, fix the reported cause, and resume.

## Migration 0043 stops on its lock timeout

`0043_attr_index_teardown` drops `okg.attr_index` and its three side tables
(upgrade order in the
[CLI reference](../../reference/cli.md)). It waits at most 5 seconds for each
lock, then fails and changes nothing. Two holders are expected:

- a step-3 MCP server or worker still running: stop it, then rerun
  `okg migrate --apply`;
- an anti-wraparound autovacuum on `okg.attr_index` (`pg_stat_activity` shows
  `autovacuum: VACUUM okg.attr_index (to prevent wraparound)`), which does not
  yield to the drop: wait until it finishes, then rerun the migrate.

## Reclaim index space left by a failed publish

A publish writes okg-schema tables inside one transaction. When it fails or
is cancelled, PostgreSQL rolls the rows back but keeps the index pages they
used; VACUUM then empties the table and never shrinks an index. On one
deployment this left 4.4 GB of index over two empty tables (both since
dropped, by migrations `0043_attr_index_teardown` and
`0046_attribute_copies_teardown`). `okg doctor
--check disk` reports such tables as `index_larger_than_table`.

Rebuild them with the supported command. Dry run first:

```bash
uv run okg db reclaim-index-space --deployment <deployment> --json
```

Check the listed tables and `blocked_by`, then apply when no source ingest is
running:

```bash
uv run okg db reclaim-index-space --deployment <deployment> --apply --json
```

The dry run exits 1 with `dry_run_blocked` when `--apply` would be refused.
`--apply` refuses while a publish holds the publisher lock, while a source run
is still writing, and when a table has an invalid index left by an interrupted rebuild (run the printed
`DROP INDEX CONCURRENTLY` first). It holds the publisher lock for the whole
run, so an ingest's end-of-run publish in that window fails with
`PublisherLeaseHeld`; publish again afterwards. It rebuilds with `REINDEX TABLE
CONCURRENTLY`, so reads and writes continue, and it fails with both values if
any index definition, grant, comment or row digest differs afterwards, naming
the tables it already rebuilt. A writer that starts during the run makes it
fail with `reindex_failed` (it drops the invalid `_ccnew` copies its own
rebuild left and prints the drop command for any it could not) or
`fingerprint_changed`; rerun it when the writer has finished. A
successful publish allocates about the same index space again, so this buys
headroom against the storage guard; it is not a permanent saving.

## Index rebuild after a publish

A btree index that a publish fills row by row ends up about a third to a half
larger than the same index built in one pass. After a successful
publish, a post-publish step rebuilds such indexes with
`REINDEX INDEX CONCURRENTLY`, one at a time. The publisher worker, `okg run --once` (with or without `--inline`),
`okg ingest` (once, after its last layer), the source-sync workflow and
provisioning (both through `okg ingest`) and merge-back do it. The MCP
connector ingest and the exchange receiver do not, so a request never waits
for it; the next worker or run cycle does the work. It acts only after the
publish has committed and released its locks. A rebuild runs when an index is
at least 1.5 times its estimated packed size and that is at least 64 MiB.

Each step (one `ANALYZE`, one index rebuild, build or drop) takes the
publisher lock without waiting, so it never runs beside a publish. Before
each step the rebuild stops when:

- a publish is waiting for the publisher lock, or holds it;
- a source run is writing;
- another session of this database holds a snapshot older than every
  transaction still running (its `backend_xmin` precedes the rebuild's own)
  and, where its start time is readable, has been open longer than the lock
  timeout (`old_snapshot_open`); every concurrent index statement would wait
  for it;
- its time budget is spent (`time_budget`, 30 s by default).

The lock timeout (5 s by default) bounds each lock wait, not the whole
statement. One `REINDEX INDEX CONCURRENTLY` waits several times (for lockers
of the table, then for older snapshots), and a failed one is followed by the
cleanup drop, which waits too: with a reader that arrived mid-rebuild and a
1 s timeout, one measurement took 2.0 s under the publisher lock. Background
workers (pg_cron, TimescaleDB jobs) are not seen by the old-snapshot guard,
but a wait for them is bounded the same way. A publish that starts during the
rebuild therefore waits for the one step in progress, never for the rest. A
publish queued behind the worker's rebuild waits at most the budget plus one
step. On one large production graph the largest index took about 10-15 s (3.5 s
on a graph a quarter of that size). An error never fails the publish.

Read what it did in the publish summary (`post_publish_index_rebuild`) or the
`post-publish index rebuild:` log line. `status: stopped` with `reason:
publish_waiting`, `publish_running`, `source_run_in_progress`,
`old_snapshot_open`, `time_budget` or
`lock_timeout` means it yielded; the next successful publish continues where
it stopped. After three runs in a row stopped by `old_snapshot_open`, the
publisher worker logs `post-publish index rebuild starved` at WARNING with
the blocking sessions' pid and age: end or shorten that transaction (an idle
session left in a transaction, a long report), and the next publish rebuilds.

Without superuser or `pg_read_all_stats`, the rebuild cannot see other roles'
transaction start times or which table another role's index build is on.
`stats_visible: false` in the report says so. The old-snapshot guard then
decides on `backend_xmin` alone. When an index build it cannot attribute is
running, every invalid index is listed under `unverifiable`: never reported
as a leftover, never dropped, and its table waits for the next run.

Each run first drops an invalid `<index>_ccnew` or `<index>_ccold` copy that
an earlier rebuild of the same table left (a worker that crashed, a cleanup
that timed out), unless an index build is in progress on that table
(`cleaned_up_leftovers`). A table with an index build in progress, such as a
migrate's, is left for the next run (`builds_in_progress`).
`leftover_invalid_indexes` lists any other invalid index with its drop
command. The table that carries it is not rebuilt until you run that command.

To turn the rebuild off, set `OKG_POST_PUBLISH_INDEX_REBUILD=0` in the
environment of the publisher worker, `okg run --once` and `okg ingest`. To
tune it, set `OKG_POST_PUBLISH_INDEX_REBUILD_RATIO` (at least 1.0),
`OKG_POST_PUBLISH_INDEX_REBUILD_MIN_BYTES`,
`OKG_POST_PUBLISH_INDEX_REBUILD_LOCK_TIMEOUT_SECONDS` (default 5) and
`OKG_POST_PUBLISH_INDEX_REBUILD_BUDGET_SECONDS` (default 30). When another
session holds an old snapshot (`reason: old_snapshot_open`), the run waits and
checks again before it gives up: at most
`OKG_POST_PUBLISH_INDEX_REBUILD_OLD_SNAPSHOT_RETRIES` times in all (default 3;
0 stops at once), after
`OKG_POST_PUBLISH_INDEX_REBUILD_OLD_SNAPSHOT_RETRY_WAIT_SECONDS` (default 1,
doubling: 1, 2, 4 s), and never past the budget. The waits come before the
publisher lock is taken. The payload lists each check under
`old_snapshot_waits`. A transaction that stays open makes every cycle that
has rebuild work wait the full 7 s before the run gives up; set the retries to
0 to stop at once. A retry count must be a whole number and the wait finite
and above 0. A value that is
not a number is reported as `status: error`; it is never replaced by the
default.

## Retired graph indexes

`okg migrate` drops seven graph read indexes that no query needs and builds one
narrower replacement. It uses `CREATE INDEX CONCURRENTLY` and `DROP INDEX
CONCURRENTLY` before its schema transaction, so graph reads and writes
continue:

- `edge_src_visible`: `edge_identity_visible` serves `src = ?` and
  `src = ANY(...)`.
- `edge_live_lo_visible`, `node_live_lo_visible`: the delta-segment indexes
  lead with the same column.
- `edge_live_hi_visible`, `node_live_hi_visible`: no read filters on
  `live_hi` alone.
- `edge_source_order_visible`: the record-set reads order by `edge_id`
  instead.
- `edge_source_run_visible`: replaced by `edge_source_run_key_visible`
  (`source, run_id, live_lo, live_hi, edge_id`), which serves the run count
  and the run's keys index-only.

### Upgrading while publishes run

A concurrent index build waits for every transaction that was open when it
started. A long publish can therefore keep it waiting past
`OKG_MIGRATE_LOCK_TIMEOUT_MS`. The migrate does not fail then:

- the index step drops the half-built (invalid) index it left;
- the step reports `graph_read_indexes.status: deferred` with `reason:
  lock_timeout`, plus the indexes still `pending`;
- the rest of the migrate succeeds, and the missing index is not built the
  blocking way.

A second migrate that runs while another index change is in progress (a
first migrate, `okg db cold-history`) reports `reason:
another_index_change_running` and does nothing to the indexes.

The pending work finishes on the next migrate, or after the next successful
publish, when the post-publish rebuild builds and drops the pending indexes
one at a time under the same rules as its rebuilds. With the rebuild turned
off (`OKG_POST_PUBLISH_INDEX_REBUILD=0`), only the next migrate finishes it. To finish it at once,
rerun `okg migrate` when no publish is running. Until then the reads the new
index serves (a source run's count and keys) use the old index if it is
still there, or another index.

An invalid index left under a new index's name by an interrupted build is
dropped and rebuilt by the next run.

## Reclaim host storage

Use this section when `okg doctor --check host-headroom` crosses the deployment
floors in `host_storage.warn_available_pct` or `blocker_available_pct`.

### How the host storage tiers nest

```
host APFS volume
 └─ Docker.raw            (sparse VM disk file; apparent size ≠ allocated host blocks)
    └─ Docker Desktop VM filesystem
       └─ docker volumes + images + container layers + logs
```

Host and VM headroom answer different questions. The host-headroom check uses
host free space to decide whether readiness can pass; the VM's `df` result says
whether data operations still fit inside the guest filesystem. Neither number
substitutes for the other. Measure both before a cleanup that writes to
Docker storage.

The measured behavior is not a reclaim guarantee:

1. **In-VM deletion can temporarily consume host space.** `DROP DATABASE`,
   file deletion inside containers, layer pruning, and Docker volume removal
   free blocks in the VM, but metadata and filesystem work can first grow the
   sparse image. Do not start a cleanup when the host cannot tolerate that
   temporary write, even if the VM reports ample free space.
2. **Whole-volume deletion can behave differently, but is not guaranteed to
   return host bytes immediately.** Reported outcomes are platform- and
   version-specific: neither a promise that `docker volume rm` shrinks the host
   file nor proof that a later restart caused compaction.
3. **APFS snapshots can pin space.** On macOS, inspect local snapshots with
   `tmutil listlocalsnapshots /` when deleted data is not reflected in host
   free space. Snapshot presence alone does not prove a snapshot pins the
   Docker image; correlate host allocation and snapshot lifecycle before
   attributing the missing space. Do not delete snapshots casually as a
   storage workaround.
4. **Compaction is platform- and version-dependent.** A complete Docker daemon
   shutdown or restart may compact the sparse image on one Docker Desktop
   release, while a partial quit or another platform may not. Treat
   shutdown/compaction behavior as an environment-specific fact to measure,
   not a universal reclaim step.

### Write-ahead log space during a large import

Three graph Postgres launchers start with `max_wal_size=4GB` (set in
`src/okg/substrate/db/wal_settings.py`): the workspace graph's dogfood
database (`okg dogfood service render`), sealed-environment databases, and the academic-researcher profile compose. On
those, a large import can hold about 4.5 GB of write-ahead log in `pg_wal` on
top of the database (4.29 GB measured); leave that much free before starting
one. `ops/pg/docker-compose.yaml` gets only `wal_compression=zstd` and keeps
the 1GB default cap, and the Kubernetes chart gets none of these settings.

The settings are passed as `-c` flags, which override `ALTER SYSTEM` and
`postgresql.conf`. Change them in the rendered compose file or the
environment's `database.wal` block, then recreate the container (`docker
compose up -d`; `restart` keeps the old command).

### Reclaim ladder

1. **Measure before any deletion.** Record host free space, VM `df` free space,
   Docker usage, and (on macOS) `tmutil listlocalsnapshots /`. If host headroom
   is too low to absorb temporary sparse-image growth, stop. Create host space
   outside Docker, expand or relocate the Docker disk image, or otherwise add
   physical capacity before any cleanup that writes to Docker storage. An
   approval alone does not make an operation fit.
2. **Delete only an exact, confirmed whole-volume target.** Run `docker volume
   inspect <name>`, map the volume to its deployment and owner, and verify a
   recovery copy or written confirmation that the data is disposable. Stop if
   any identity or recovery fact is unknown. Only then run `docker volume rm
   <name>`. Check `okg db gc --cluster-dsn <dsn> --json` (dry-run default) for
   leaked `okg_test_*` / `okg_upgrade_verify_*` / `okg_golden*` databases
   first; `--apply` drops only those allowlisted prefixes. Re-measure host and
   VM free space afterward; neither immediate host reclaim nor later compaction
   is guaranteed.
3. **Prefer targeted cleanup to a system-wide prune.** If the owner explicitly
   chooses `docker system prune`, keep Docker's confirmation prompt enabled and
   review its full scope: stopped containers, dangling images, unused networks,
   and unused build cache. Do not add `--volumes`.
4. **Use only the installed platform's supported reclaim procedure.** Positively
   identify Docker Desktop and its installed version before using a VM trim or
   disk-reclaim path. Do not apply a privileged PID-host namespace recipe to
   native Linux: there, PID 1 is the actual host rather than a Docker Desktop
   VM. Preserve command errors and compare host allocation with VM free space
   before and after. If the installed platform has no documented procedure, or
   the result is unclear, stop; do not treat trim as successful.
5. **Shrink what the VM genuinely holds** — the database-side levers: the attribute-interval sidecar prune,
   telemetry compression, cold-history compression, and the disk report
   (`okg metrics --deployment <d> --disk`) to attribute before acting.
6. **Backups and venvs** (host-side, outside Docker): `okg up` retention keeps
   last-N verified dumps (`--backup-keep`, default 1); superseded archives in
   `.okg/backups/<deployment>/` are safe to delete by hand when older than the
   last verified one. `python scripts/gc_worktree_venvs.py` (dry-run default)
   removes regenerable `.venv` dirs in merged worktrees.

### Cadence

- Continuous: the scheduled doctor (no action needed — it fires the blocker).
- After any bulk deletion (janitor `--apply`, volume rm): re-measure both host
  and VM headroom, then use the applicable trim/compaction path. Bytes may
  return later, remain pinned by snapshots, or not be reclaimable until live
  data is reduced.
- When the WARN floor trips: run the ladder top-down before it becomes a
  blocker; attribute first with `okg metrics --disk` if the cause is unclear.

### Verify host recovery

Re-run the host-headroom check and `okg metrics --deployment <d> --disk`.
Recovery is complete only when the available-space percentage is above the
warning floor, the database remains healthy, and the next scheduled doctor
does not report a blocker.
