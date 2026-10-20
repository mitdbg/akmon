---
audience: operator
type: how-to
canonical_id: runbook-runtime-queue-recovery
---
# Recover runtime queue failures

Use this procedure when pending DBOS work grows, active work becomes stale, or
work has exhausted its retries. Queue state is authoritative in
`okg.v_runtime_queue_health`; DBOS workflow history is immutable evidence.

## Inspect the queue

```bash
uv run okg dogfood status --deployment <deployment> --json
uv run okg jobs status --deployment <deployment> --json
uv run okg sync health --deployment <deployment> --json
uv run okg db status --deployment <deployment> --brief
```

For exact counts and rows:

```sql
SELECT * FROM okg.runtime_queue_health(600);
SELECT workflow_uuid, name, status, queue_name, error, created_at, updated_at
  FROM dbos.workflow_status
 WHERE queue_name = 'okg-<deployment>'
 ORDER BY created_at DESC
 LIMIT 50;
```

Do not infer the current DLQ from cancelled rows. Only `ERROR`, `FAILED`,
`MAX_RECOVERY_ATTEMPTS`, and `MAX_RECOVERY_ATTEMPTS_EXCEEDED` are failures.
`CANCELLED` replaceable wakeups are retained history and do not make the queue
unhealthy.

## Pending or stale work

- For backed-up `okg_publisher_cycle` work, increase its schedule interval or
  inspect the slow generation with `okg generation describe <generation>`.
- For failing source sync, inspect source progress and rerun only that source
  through the runtime path:

  ```bash
  uv run okg ingest --deployment <deployment> \
    --include <source> --mode reconcile
  ```

- For embedding work, confirm `embeddings.enabled`, its backend configuration,
  and that `okg runtime worker --deployment <deployment>` is running.

Do not insert, update, or recreate legacy `okg.jobs` rows.

## Work that exhausted retries

Do not delete or rewrite a terminal DBOS row. Fix the cause and create a newer
successful witness for the same responsibility. When a promotion fence is
active, keep the fence and enqueue recovery under its exact owner:

```bash
uv run okg runtime enqueue-fenced-recovery \
  --deployment <deployment> doctor_workflow \
  --promotion-id <active-promotion-id> --json
```

The returned workflow must reach `SUCCESS`. Source-sync, resource-monitor, and
git-reconcile failures require a successful replacement of the same kind and
the exact published generation or durable sample it produced.

If DBOS must retain the old error after that verified recovery, inspect the
current failure projection:

```bash
uv run okg db current-runtime-dlq --deployment <deployment> --json
```

Create an acknowledgement request containing the operator actor and reason,
exact failed workflow UUIDs, candidate revision, active promotion receipt,
current readiness result, and replacement workflow IDs. Dry-run it, review the
returned plan, then apply only that digest:

```bash
uv run okg db acknowledge-runtime-dlq \
  --deployment <deployment> --request acknowledgement.json --json

uv run okg db acknowledge-runtime-dlq \
  --deployment <deployment> --request acknowledgement.json \
  --confirm-plan-id sha256:<digest> --apply --json
```

Use the deployment operator/admin database role. Application and catalog roles
cannot write the acknowledgement ledger. Stop if the plan is stale, a workflow
type is unknown, the promotion owner changed, a replacement is unhealthy, or
the failure snapshot drifted. Never substitute booleans in the request for the
live workflow, source, generation, watermark, or readiness evidence.

## Verify recovery

```bash
uv run okg db current-runtime-dlq --deployment <deployment> --json
uv run okg deployment ready <deployment> --checks general --json
uv run okg jobs status --deployment <deployment> --json
```

Recovery requires a healthy queue, no stale active work, no newly growing
failure count, and `current_count: 0`. Historical, acknowledged, superseded,
and cancelled rows remain visible.
