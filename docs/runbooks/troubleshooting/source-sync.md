---
audience: operator
type: how-to
canonical_id: runbook-source-sync-not-firing
---
# Runbook: source sync not firing


**Alert source.** A file, git ref, webhook, or poll signal should have
refreshed a source, but `node_facts` / `edge_facts` did not change, the
next generation did not publish, or downstream graph reads still show
old state.

The [maintained-knowledge lifecycle](../../start/data-lifecycle.md) explains
why a sync wakeup, an accepted working-state observation, and a published
generation are different events. At this boundary, the source is authoritative
for what it currently reports, not whether the report is factually true. A
wakeup never substitutes for reading it.

## Mental model

Source sync events are wakeups; the source state determines what the adapter
observes. The adapter's cursor, `okg.source_progress`, fingerprints, and record
set determine whether new insert or retraction facts are emitted.

Do not hand-write live graph rows to "fix" sync. Recover by making the
producer run source sync through the DBOS runtime path or inline source runner,
then let `SourceRunner` and the publisher converge the graph.

## Triage

1. **Confirm the source declares sync capability.**

   ```bash
   uv run --extra dev okg sync explain --deployment <deployment> --json
   uv run --extra dev okg sync explain --deployment <deployment> \
     --path <changed-path> --trigger fs_event --json
   uv run --extra dev okg doctor --deployment <deployment> --check sources --json
   ```

   Check `deployments/<deployment>/source_registry.yaml` for the source's
   `sync.triggers`, `sync.watch.roots`, include/exclude globs, and
   `sync.reconcile_mode`. The sync explanation should show the matched
   source, grouped run mode, dependent fanout, scoped execution support,
   delete/reconcile fallback, compaction policy, webhook path, and poll
   probe. For file or git-ref producers, the watched root must exist on
   the host running `okg runtime worker` (the embedded filesystem
   watcher inside the worker is the only watch path per
   `consolidate-deployment-runtime`).

   File-backed built-ins accept path scope only when their adapter can
   validate the key against configured roots or explicit artifact lists.
   Scoped families cover repository/OpenSpec/document files, agent transcript
   JSONLs, CI and quality artifacts, agent-memory packets, and
   transcript-derived remote-host evidence. Local machine environment probes
   take no path scope; use poll/manual/reconcile, because the source truth is
   the current host/runtime/MCP snapshot.
   For deletes and moves, expect `deletes_require_reconcile=true` and a
   `scope_complete` reconcile fallback unless the source explicitly owns
   scoped retraction semantics.

2. **Check queue depth and DLQ.**

   ```bash
   uv run --extra dev okg metrics --qa-snapshot --deployment <deployment> --json
   uv run --extra dev okg sync health --deployment <deployment> --json
   ```

   Then inspect recent DBOS workflows:

   ```sql
   SELECT workflow_uuid, name, status, queue_name, created_at, updated_at
     FROM dbos.workflow_status
    WHERE name = 'okg_source_sync'
      AND queue_name = 'okg-<deployment>'
    ORDER BY created_at DESC
    LIMIT 20;
   ```

   If DBOS workflows back up, start or scale
   `okg runtime worker --deployment <deployment>`. If failures appear,
   inspect the workflow error payload first; invalid inputs should carry stable
   source-sync error codes.

3. **Check source watermark and progress.**

   ```sql
   SELECT source, last_applied_rev, last_reconcile_at, projection_cursor
     FROM okg.source_watermarks
    WHERE source = '<ownership_id>';

   SELECT key, value, updated_at
     FROM okg.source_progress
    WHERE deployment = '<deployment>'
      AND source_name = '<ownership_id>'
    ORDER BY updated_at DESC
    LIMIT 20;
   ```

   If the watermark/progress did not move after a completed job, the source
   decided the upstream state was unchanged, or its fast path is stale.

4. **Check record-set quality for reconcile sources.**

   For deployments with record-set doctor checks, run:

   ```bash
   uv run --extra dev okg doctor --deployment <deployment> --check <deployment> --json
   ```

   Inspect record-set stale/missing node and edge counts. Stale rows mean
   the source did not declare the completed scope correctly; missing rows
   usually mean record identity changed or the source's record-set cursor
   is corrupt.

5. **Force a manual reconcile.**

   ```bash
   uv run --extra dev okg ingest \
     --deployment <deployment> \
     --include <source-name> \
     --include-dependents \
     --mode reconcile
   ```

   A successful reconcile should leave DBOS workflow failures at zero, source
   watermark/progress updated, and the next generation published. Use
   `--enqueue` only when the deployment has the DBOS runtime enabled and a
   runtime worker is running; the legacy `okg.jobs` enqueue path is retired.

   For webhook or poll producers, test the producer directly:

   ```bash
   uv run --extra dev okg sync webhook \
     --deployment <deployment> \
     --source <source-name> \
     --payload-json '{"scope_key":"path/to/file.md"}' \
     --signature sha256=<hmac> \
     --json

   uv run --extra dev okg sync poll \
     --deployment <deployment> \
     --source <source-name> \
     --json
   ```

   The webhook **server** is no longer a separate CLI; it runs
   inside the worker. Configure it via
   `runtime.source_sync.webhook` in the deployment manifest, then
   restart the worker. For one-shot triage of a single payload,
   `okg sync webhook` above remains available.

6. **Confirm graph convergence.**

   ```bash
   uv run --extra dev okg generation describe <generation_id>
   uv run --extra dev okg metrics --qa-snapshot --deployment <deployment> --json
   uv run --extra dev okg sync health --deployment <deployment> --json
   uv run --extra dev okg doctor --deployment <deployment> --check <deployment> --json
   ```

   Compare `nodes_live` / `edges_live` row deltas for the published
   generation. If rows still do not change after a reconcile and the
   source state really changed, debug the adapter's cursor, fingerprint,
   and record-set calculation before changing publisher or MCP code.

## Common causes

- Watch root does not exist on the host running `okg runtime worker`.
- Include/exclude globs filter the changed path.
- Runtime worker is not running the `okg_source_sync` workflow queue.
- A DBOS workflow with the same desired source-sync frontier is already
  active or waiting.
- Source fast path sees stale upstream state.
- `source_progress` contains a stale per-record fingerprint.
- `last_applied_rev->'record_set'` is malformed or no longer matches the
  source's record identity.
- Publish is blocked after ingestion; use
  [`generation-blocked.md`](generation-blocked.md).

## Reading `okg sync frontier`

`okg sync frontier --deployment <deployment> --json` reports one
`read_visibility_state` per source. The report says `attention_required`
only when a person has to act: some source is `frontier_state: blocked`
or has a state in the "Acts" column marked yes.

| `read_visibility_state` | Meaning | Acts |
| --- | --- | --- |
| `read_visible` | The published generation holds the source's applied work. | no |
| `unpublished_writes` | Applied work the next publish takes (ingest with publish off, an embedding write, a lineage repair). `frontier_state: publish_pending`. | no |
| `frontier_advancing` | A source-sync run is in flight. | no |
| `last_run_failed` | The newest run failed (`last_attempt_status`, `last_attempt_run_id`). Rows from an earlier run may still be visible. Fix the source and rerun it. | yes |
| `published_state_unavailable` | The published checkpoint's state is missing, so visibility cannot be judged. | yes |
| `publish_blocked` | The source has applied work the published generation lacks, and the deployment's newest generation is blocked, rejected, failed or quarantined. The next publish will not take the work until the cause is fixed; see [`generation-blocked.md`](generation-blocked.md). `frontier_state: blocked`. | yes |
| `replay_required` | The source owes a full replay. | yes |
| `record_set_blob_missing` | The cursor names a record-set blob that is gone. | yes |
| `no_watermark`, `no_published_generation` | The source never applied work, or the deployment never published. | yes |

## QA Checklist

Before enabling scoped sync for another built-in source family, add or
confirm QA coverage in these layers:

- Static policy matrix: `sync.triggers`, watch roots, include/exclude
  globs, `scope.accepts`, event fanout, reconcile fanout, compaction,
  and delete/reconcile fallback are asserted from the loaded source
  registry.
- Operator diagnostics: `okg sync explain` or
  `explain_source_sync_event` shows the representative changed path,
  matched scope key, `scope_incomplete` update mode, path-scoped
  execution when supported, and `scope_complete` reconcile fallback for
  deletes/moves.
- Adapter safety: externally supplied path scopes are resolved under the
  configured root or artifact allowlist; `../escape` and unconfigured
  files emit no facts.
- Workflow durability: a scoped event that collides with active source-sync
  work records a DBOS wakeup/successor instead of being dropped.
- Producer durability: poll fingerprints advance only after enqueue
  succeeds; webhook delete/remove payloads use reconcile mode when the
  source declares `deletes_require_reconcile`.
- Graph smoke: before marking a broad source-sync task complete, record
  the published generation status, source fact deltas, live row deltas,
  queue depth, DLQ count, metrics JSON, and doctor result for the
  affected source family.

## Verify recovery

Trigger one bounded source-sync event and confirm the workflow succeeds, the
source watermark advances, facts are admitted, and a published generation
contains the expected change. Record queue and doctor output with the result.

## Escalation

If DBOS workflow status, DLQ/backlog, source watermark, source progress,
record-set quality, and doctor output are all clean but the graph is stale,
capture the producer payload, workflow id/status/error, source watermark row,
progress rows, latest generation status, row deltas, and doctor JSON in the
evidence packet before changing code.
