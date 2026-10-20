---
audience: operator
type: how-to
canonical_id: runbook-source-poisoning
---
# Runbook: source's fast path returning garbage


**Alert source.** DBOS source-sync failures spike for one specific
source; or downstream invariants start
failing mentioning rows from one source; or a deployment owner
reports visibly-wrong data in MCP responses.

---

## What "poisoned" means

A source's fast path is producing records that are:

- malformed (fail the adapter's record validation)
- semantically wrong (pass validation, but represent reality
  incorrectly — e.g. a bug in the adapter emits issue `#42` for
  the wrong repo)
- stale (the source's upstream API started returning old snapshots)

The substrate's dedupe + append-only discipline means poisoned
data never overwrites good data; recover by re-observing from a
known-good point.

---

## Diagnose

1. **Confirm it's one source, not systemic.**

   Check recent source-sync workflow failures and source progress:

   ```sql
   SELECT workflow_uuid, status, created_at, updated_at
     FROM dbos.workflow_status
    WHERE name = 'okg_source_sync'
      AND queue_name = 'okg-<deployment>'
    ORDER BY created_at DESC
    LIMIT 20;

   SELECT source_name, key, value, updated_at
     FROM okg.source_progress
    WHERE deployment = '<deployment>'
    ORDER BY updated_at DESC
    LIMIT 20;
   ```

   If one source dominates, confirmed.

2. **Sample a bad record.**

   Read the source-sync workflow error payload or the source adapter logs.
   The error usually points at adapter validation or the writer path's
   narrowing check.

3. **Check if poisoned data made it into a published read.**

   ```sh
   uv run okg --json trace node <node_id> \
     --deployment <deployment> --generation-id <generation-id>
   ```

   For source-record-level tracing, query the fact log directly:

   ```sql
   SELECT generation_id, fact_kind, attrs
     FROM okg.node_facts
    WHERE source = '<source>'
      AND source_record_id = '<source_record_id>'
    ORDER BY generation_id DESC;
   ```

   Use the trace's pinned published read section to determine whether a reader
   could see the record. A fact-log entry without a corresponding published
   read was not reader-visible at that generation.

---

## Respond

### Step 1: stop the bleeding

Mark the source disabled in `deployments/<name>/source_registry.yaml`
so no new facts arrive while you investigate:

```yaml
sources:
  <offending_source>:
    enabled_by_default: false   # was true
```

Then re-publish with the updated registry:

```sh
uv run okg ingest --deployment <name> --inline --json
```

### Step 2: fix the cause

Most common causes and fixes:

- **Adapter bug:** patch the adapter code, deploy the new worker
  image. The substrate validates records.
- **Upstream API regression:** work around in the adapter or
  coordinate with the upstream owner.
- **Misconfigured `record_identity_fields`:** check the source's
  YAML at `library/profiles/<profile>/source-defaults/<src>.yaml`
  vs. `deployments/<name>/source_registry.yaml`; fix and run
  `okg catalog load --deployment deployments/<name> --apply`.

### Step 3: replay to re-observe from a known-good point

Find the watermark **before** the first poisoned record:

```sql
SELECT source, last_applied_rev, last_reconcile_at
  FROM okg.source_watermarks
 WHERE source = '<offending_source>';
```

Pick a timestamp/revision that predates the bug (often easy: the
bug started at time T; any rev before T is safe).

```sql
-- Roll back the watermark so the next reconcile re-observes
-- from the known-good point.
UPDATE okg.source_watermarks
   SET last_applied_rev = '<pre-bug-rev>',
       last_reconcile_at = NULL
 WHERE source = '<offending_source>';
```

Then trigger a reconcile-mode ingest:

```sh
uv run okg ingest --deployment <name> --include <offending_source> \
  --mode reconcile --inline
```

The reconcile-mode job re-observes records from the watermark
forward. Good records already in the fact log dedupe (noop).
Changed records produce new revision rows. Disappeared records
retract if the source's profile permits.

### Step 4: resume

Re-enable the source in `source_registry.yaml`:

```yaml
sources:
  <offending_source>:
    enabled_by_default: true   # restored
```

Verify:

```sh
uv run okg generation list --deployment <name> --limit 3 --json
# next published generation should include the source with clean
# invariants
uv run okg metrics --qa-snapshot --deployment <name> --json
# the DLQ count is bounded and no new source failure appears
```

---

## What to do about the poisoned rows that are already published

They're retractable without a rebuild. Two paths:

### If the poisoned records were deleted upstream

The replay's reconcile run will emit retraction facts naturally —
those records are "missing from completed scope." No extra work.

### If the poisoned records still exist upstream but are just wrong

Replay won't retract them (they're still there). Instead:

- The corrected adapter will emit new revision facts, which
  supersede the old ones per the revision-kind rule.
- The old bad revisions remain in the fact log (history). They're
  audit-visible via `okg trace node` but invisible to pinned
  readers after the new revision publishes.

---

## Prevent recurrence

- **Record-level validation at the adapter.** The adapter's
  record-emit path should reject malformed records up front
  rather than relying on the writer path's narrowing check.
- **Dedupe_key collision audit.** A poisoned source that emits
  different payloads for the same `(source_record_id,
  source_revision)` will dedupe-collide on the same `dedupe_key`
  and the second arrival is silently dropped: a corrected arrival
  reusing the same revision looks already-seen. Bump the
  `source_revision` so the new fact has a distinct dedupe_key —
  the substrate's bitemporal design means there is no fact
  eviction.
- **Per-source canary.** Run `uv run okg ingest --deployment <name>
  --include <src> --inline` weekly against a fixture deployment.
  A drift there is an early-warning.

---

## Verify recovery

Re-run the per-source canary and inspect the source watermark, dedupe result,
and published generation. Recovery is complete only when the corrected
revision is visible and no new poison or dead-letter signal appears.

## Escalation

If multiple sources poison simultaneously, it's not a source bug —
it's probably an adapter-framework regression or a network
event. Page the substrate team.
