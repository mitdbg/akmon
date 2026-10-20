---
audience: operator
type: how-to
canonical_id: runbook-database-management
---
# Database Management


Use this runbook when a deployment database grows unexpectedly, when
`okg doctor --check database` reports drift, or before destructive cleanup.

Write every DSN below -- `OKG_DSN`, a manifest's `postgres.dsn`, a `--dsn`
flag -- in URL form, `postgresql://user:password@host:port/dbname`. The
keyword form libpq also accepts, `host=... dbname=...`, is refused: the graph
selects its storage by URL scheme, so a scheme-less string would be taken for
a file path. Commands now refuse it by name and print the URL to use instead.

## Inspect

Start with the deployment-scoped view:

```bash
okg db status --deployment my-graph --brief
okg db status --deployment my-graph --json
okg doctor --check database --deployment my-graph --json
```

The status report covers database size, latest generation, top relation
families, Timescale policy, retired legacy-runtime diagnostics, DBOS workflow
backlog, derived relations, and migration drift. Backlog rows carry the
observed timestamp column, an ISO-normalized oldest timestamp, age in seconds,
and `dbos.workflow_status` size, normalized even when DBOS stores epoch
milliseconds.

## Inventory

Manifest-scoped inventory is the default:

```bash
okg db list --json
```

Whole-cluster scans are explicit because they touch every visible database:

```bash
okg db list --cluster --cluster-dsn "$MAINTENANCE_DSN" --json
```

Warnings named `mixed_deployment_state` mean one database has published rows
for more than one non-bootstrap deployment in `okg.graph_generations`. That
violates the one-db-per-deployment model unless the database is a disposable
fixture.

## Dry-Run Cleanup

Cleanup discovery is dry-run first and should not change the database count:

```bash
before=$(psql "$MAINTENANCE_DSN" -Atc "SELECT count(*) FROM pg_database")
okg db gc --dry-run --cluster-dsn "$MAINTENANCE_DSN" --json
after=$(psql "$MAINTENANCE_DSN" -Atc "SELECT count(*) FROM pg_database")
test "$before" = "$after"
```

Only scratch/test/bench/dev-classified databases without manifest owners
qualify. The helper refuses protected names such as `postgres` and `okg`.

## Migration Drift

Check the ledger before and after schema work:

```bash
okg db migrations --deployment my-graph --json
```

`checksum_mismatches` means the installed ledger no longer matches the
migration file. Stop and inspect before migrating again. `destructive_pending`
is informational: destructive migrations are operator-run, never applied by
plain `okg migrate`.

## DBOS Backlog Cleanup

Start with a dry-run. The command targets only stale `PENDING` and `ENQUEUED`
rows, reporting the exact groups before any mutation:

```bash
okg db dbos-gc --dsn "$OKG_DSN" \
  --stale-after-seconds 86400 \
  --json
```

To narrow it, add `--queue-name okg-my-graph`, `--workflow-name
okg_source_sync`, or `--status` flags. Confirmed cleanup marks eligible rows
`CANCELLED` rather than deleting them, so history stays inspectable:

```bash
okg db dbos-gc --dsn "$OKG_DSN" \
  --stale-after-seconds 86400 \
  --confirm --apply --json
```

## Remove Obsolete Job Runtime

Run the cleanup only after every kept deployment has passed the DBOS soak
gate:

```bash
okg db remove-job-runtime-subsystem --dsn "$OKG_DSN" \
  --confirm --apply --json
okg migrate --dsn "$OKG_DSN" --apply
okg db status --dsn "$OKG_DSN" --brief
```

The command aborts before dropping objects if DBOS work is active or either
generation of OKG job tables has queued, leased, running, or retryable rows.
It records `0002_remove_job_runtime_subsystem` in `okg.schema_migrations` and
proves the obsolete tables, views, functions, markers, and lease table absent.
Later `okg migrate` runs never recreate them.

### When the legacy rows cannot drain

The retired job subsystem has no worker, so rows still marked `queued`,
`running`, or `retryable` never clear and the abort above becomes permanent.
Dump the tables first, then decide explicitly:

```bash
pg_dump "$OKG_DSN" --data-only -t 'okg.job_*' > ~/legacy-job-runtime.sql
okg db remove-job-runtime-subsystem --dsn "$OKG_DSN" \
  --confirm --apply \
  --accept-retired-ledger-rows \
  --retired-rows-dump-ref ~/legacy-job-runtime.sql --json
```

`--accept-retired-ledger-rows` requires `--retired-rows-dump-ref`, which must
name a readable non-empty file (or a remote URI), and relaxes only that one
refusal — the DBOS drain check and every postcondition still apply. The
per-relation, per-status row counts it destroyed — every row in every dropped
table, not only the ones that blocked it — and the dump reference are written
into the migration's ledger metadata.

### Verifying a recorded migration against reality

`okg db migrations` re-checks each applied destructive migration's recorded
postcondition against the live catalog and reports `postcondition_drift`. A
recorded claim the database contradicts — for example a relation recorded
absent that still holds rows — names the migration, the flag, the relation, and
its current row count, and turns the status `ok` false. This is why a green
migration ledger is evidence rather than an assertion: migration `0001` once
recorded seven tables absent, re-created by the next schema apply, and nothing
looked again for a month.

The end state is strict:

- Fresh or development databases do not install OKG job runtime tables.
- The empty queue compatibility view has been removed. Code that needs queue
  health must read `okg.v_runtime_queue_health` or `okg db status`; missing
  DBOS state is reported as unavailable rather than an empty healthy queue.
- New runtime code must not add dependencies on OKG job tables.

## Legacy Graph Storage Retirement

Chronos-native deployments use Chronos interval storage as the graph source
of truth. The default legacy graph retirement path is only the compatibility
live-table cleanup for `okg.nodes_live` and `okg.edges_live`; it is not an
archival purge.

Before mutation, callers should report
`legacy_graph_storage_retirement_plan(conn)`. The report lists the exact
destructive targets and row counts, and separately names preserved relations
such as `okg.node_facts`, `okg.edge_facts`, `okg.graph_generations`,
`okg.branches`, `okg.branch_merge_requests`, `okg.edge_fact_strandings`, and
the current read views. Any cleanup that touches those audit or branch
surfaces must be introduced as a separate operator action with its own
dry-run output.

## Destructive Drops

Dropping a database requires a name, a cluster DSN, and both `--confirm` and
`--apply`. Without `--apply` the command dry-runs:

```bash
okg db drop --name okg_tmp_example \
  --cluster-dsn "$MAINTENANCE_DSN" \
  --confirm --apply
```

Manifest-owned databases are refused unless the operator supplies
`--override-deployment <name>`. Back up anything non-disposable first.

## Idle Transactions

okg sessions set `idle_in_transaction_session_timeout` (default 300000 ms):
a session abandoned mid-transaction is terminated and its locks freed, and the
caller sees SQLSTATE `25P03`. Override per deployment with
`postgres.idle_in_transaction_session_timeout_ms` or per process with
`OKG_IDLE_IN_TRANSACTION_TIMEOUT_MS`. The environment variable wins over the
manifest key. Sessions opened by Chronos, okg's versioned-storage layer, get the
same value.

The manifest key reaches the shared connection pool and Chronos sessions only
when `OKG_DEPLOYMENT` names the deployment in the process environment. The
CLI's `--deployment` flag does not export it, so a process started that way
gets the default unless you also set `OKG_DEPLOYMENT` or the environment
variable. `0` sends no setting at all, which leaves the PostgreSQL server's own
`idle_in_transaction_session_timeout` in force; it disables the bound only when
the server's value is also `0`.

## Schema Resolution

OKG pins the session `search_path` to `public` on every session it opens. Chronos's
physical tables live in `public`, and the substrate names them there
explicitly, so a session must not resolve an unqualified name anywhere else.

Without the pin, PostgreSQL's default `"$user", public` lets the login role's
own NAME choose the schema. A role named `okg` puts the `okg` schema — which
the migration creates — ahead of `public`, unqualified tables are created
there, and a fresh `okg migrate --apply` fails partway through. The pin is the
path such a role would have had anyway if it were named anything else, so no
other deployment changes. A `search_path` set in the DSN still overrides it.
