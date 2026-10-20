---
audience: operator
type: how-to
canonical_id: runbook-publisher-lock-held
---
# Runbook: another publish holds the lock

Only one writer changes a deployment's published graph at a time. Each
publish (the background worker's scheduled cycle, a source sync that
publishes, and a manual `okg run --once` or `okg ingest`) first takes the
deployment's publish lock, a PostgreSQL advisory lock. Some maintenance
steps take the same lock: attribute repair, history compaction, admission,
and starting a staging service. A catalog change
(`okg catalog load --apply`) takes a separate catalog lock. The database
releases either lock as soon as the session that holds it ends, including
when its process is killed.

## Symptoms

- A publish fails with error code `publisher_lease_held` and the message
  `publisher lock is held for deployment='<name>'`. It waited first: 600
  seconds by default, set by `OKG_DBOS_PUBLISH_LOCK_WAIT_SECONDS`.
- A maintenance step fails with `publisher lock is held during
  <operation>` after waiting 30 seconds.
- A publish fails with `publisher_lease_held` whose data has
  `"reason": "source_sync_active"` and a `blocking_workflow_id`. A source
  sync is publishing and has priority.
- `okg catalog load --apply` fails at once with `catalog_apply_lock_held`.
- `okg doctor --deployment <deployment> --json` reports
  `publisher_workflow_health` with `"ok": false`. The background publisher
  has not finished a cycle within twice its schedule interval. This check
  applies only to a deployment that runs the background worker.
- The worker log warns
  `publisher held the ... publish lock <n>s (> soft warn <m>s ...)`. A
  publish is running slowly; `OKG_PUBLISH_LOCK_HOLD_WARN_SECONDS` sets the
  threshold.

## Diagnose

### 1. Read the error's data

`publisher_lease_held` and the maintenance-step error carry
`advisory_key`, two integers that identify the lock.
`catalog_apply_lock_held` carries `advisory_key` too. Its `owner_meta`
describes the command that failed, not the one holding the lock; step 3
finds the holder.

### 2. If the reason is `source_sync_active`, wait

The source sync publishes first. Retry your command after it finishes.
See its progress with:

```bash
okg jobs status --deployment <deployment> --json
```

### 3. Find the session that holds the lock

Put the two `advisory_key` numbers into this query and run it with `psql`
against the deployment's database. Seeing another session's query text
needs a superuser or the `pg_read_all_stats` role; without it the `query`
column reads `<insufficient privilege>`.

```sql
SELECT a.pid, a.application_name, a.client_addr, a.backend_start,
       a.state, a.query_start, left(a.query, 120) AS query
  FROM pg_locks l
  JOIN pg_stat_activity a USING (pid)
 WHERE l.locktype = 'advisory' AND l.granted
   AND l.classid = (<first key>)::oid AND l.objid = (<second key>)::oid
   AND l.objsubid = 2
   AND l.database = (SELECT oid FROM pg_database
                      WHERE datname = current_database());
```

Keep the parentheses: a key can be negative, and `(-5)::oid` converts
it to the form `pg_locks` stores.

### 4. Decide whether the holder is working

If `query_start` keeps moving or the worker log shows progress, a publish
is running. Let it finish. If the process that owns the session is gone or
hung, treat the lock as stale.

## Recover a stale lock

1. Stop the process that holds the lock: the worker, or the command
   another operator or agent started. Ask its owner first if it is not
   yours.
2. If the process cannot be stopped and the session is still open, end the
   session from `psql`. This releases the lock and aborts that work. It
   needs a superuser, the session's own role, or the `pg_signal_backend`
   role.
3. Re-run the command that failed.

To end the session in step 2:

```sql
SELECT pg_terminate_backend(<pid>);
```

## Verify recovery

```bash
okg generation list --deployment <deployment> --limit 3
okg doctor --deployment <deployment> --json
```

Recovery is complete when your command succeeds, the newest generation in
the list is the one it published, and `publisher_workflow_health` reports
`"ok": true` (for a deployment with a background worker).

If publishes keep timing out behind a slow publish, see
[runtime queue recovery](runtime-queue.md) and
[publication keeps failing](generation-blocked.md).
