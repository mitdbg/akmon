---
audience: operator
type: how-to
canonical_id: deployment-runtime
section: operations
visibility: primary
status: active
owner: runtime
---
# Keep the graph current with the background worker

The background worker keeps a deployment current without you running
`okg ingest`: it checks each polled source every 5 minutes and re-reads only
a source that changed. It is off by default. Polling is set up for
deployments installed with `--profile workspace-graph` by this version; a
source without a `sync.poll` block in `source_registry.yaml` is not polled. Below, `<name>` is the deployment
and `<dir>` its folder, `~/.local/share/okg/deployments/<name>` by default.

## Turn it on

1. In `<dir>/deployment.yaml`, find the `runtime:` block (not `embeddings:`,
   which also has an `enabled:` key) and change `enabled: false` to
   `enabled: true`. Leave the poll settings the install wrote.
2. In the `postgres:` block, add `database: <the database name from the
   connection string>`. Without it, `okg deployment lint <name>` reports
   `deployment.runtime.database_undeclared`.
3. Record the edit and set up the worker's tables:

```bash
okg catalog ownership claim --deployment <name>
okg runtime bootstrap --deployment <name>
```

If bootstrap prints a fix for `okg_mcp`, that is the MCP server's login,
which `okg db role-passwords` sets; the worker does not need it. An install
made by an earlier version has no `runtime:` block or poll settings, and
these steps were not tested on one.

## Start it

```bash
okg runtime worker --deployment <name>
```

It runs in the foreground. For a long-running deployment, run it under the
system's service manager; the repository's
[systemd unit](assets/okg-runtime-worker.service) (with its
[environment file](assets/runtime-worker.env.example)) and
[launchd plist](assets/org.okg.runtime-worker.plist) are starting points,
not tested with a plain install: edit the paths, user and names, and run the
same `okg` you use for other commands. The [Kubernetes](kubernetes.md) chart
runs the worker, but on a fixed revision and without 5-minute checks.

Stopping it (`SIGTERM`) can take as long as a source read in progress; the
service files stop waiting after 30 to 45 seconds.

## Check it

```bash
okg runtime list-schedules --deployment <name>
okg doctor --deployment <name>
```

The schedules include `okg_source_poll_interval_300s`. `okg doctor` exits 1
until the worker has run about 5 minutes (it names `deployment_doctor_never_ran`
or `publisher_workflow_never_ran`), and briefly after a restart
(`publisher_workflow_stale`); then it exits 0. Paused publishing only warns
(`publisher_workflow_paused`). In test runs a new file was searchable between about 40
seconds and 5 minutes after it was committed, and edits and deletions arrived
within one check.

## What to know

- Each source's interval is `sync.poll.interval_seconds` in
  `source_registry.yaml` (300). Once a day every source is read in full
  (`runtime.source_sync.poll.backstop_multiplier: 288`).
  `codeql_data_flow` and `profile_measurements` are not polled; they are
  re-read when `git_files` changes, and by `okg ingest`.
- The scheduled publisher waits for a queued or running source sync only while
  it is current: its last publish, no-op or generation is younger than
  `publisher.max_source_sync_yield_seconds` in `deployment.yaml` (default 600).
  Past that it publishes beside the sync, still refusing a source run that is
  writing; it keeps waiting while the branch has no published generation.
  `okg doctor` reports `source_sync_stuck` for a sync row older than the bound
  (an error only while the branch has no published generation and no source
  run is writing) and `publisher_workflow_stale` (naming the newest skip) when
  nothing has published for the bound plus the 10 minute source-run wait plus
  two cron intervals. A cycle that ends in a blocked checkpoint still counts as
  fresh; the publish-block checks report the block.
- With the runtime on, `okg ingest` hands its work to the worker and waits,
  so run it only while the worker runs. Without one it waits up to an hour,
  then stops with its work still queued, and that work makes
  `okg ingest --inline` refuse (`runtime_source_sync_active`) until a worker
  runs it.
- After upgrading okg or editing `deployment.yaml` or `source_registry.yaml`,
  restart the worker: `okg doctor` says "restart it" with the command.
  `okg deployment apply` applies everything else, then exits with an error
  that names the restart, because it does not restart a worker you started.
- `okg up` refuses to upgrade while the runtime is on or a worker you started
  is running, including under the service files above.
- To go back to refreshing by hand, stop the worker, set `enabled: false`
  again and run `okg catalog ownership claim --deployment <name>`.

## Confirm the target (from a source checkout)

Before starting, upgrading, or repairing a deployment, inspect the database and
configuration AKMON will use:

```bash
okg deployment env <name> --json
```

The reported deployment, profile, and database must match the intended target.
Do not paste an old connection string into a new shell to make discovery pass.
If local development needs an explicit database, record it through the
deployment command and inspect the result again:

```bash
okg deployment env <name> \
  --write-local --dsn <postgresql-dsn> --json
```

## Deploy or upgrade from a source checkout

`okg deployment up` brings the selected deployment's database and graph up to
date with the current checkout:

```bash
# Worktree-isolated development database
okg deployment up <name> --target dev --json

# Database declared by the deployment
okg deployment up <name> --target prod --json
```

The command applies migrations, applies the catalog, performs scope-complete
ingest/publish, and reports the required runtime-worker restart. Use
`--include <source> ...` for an intentionally limited source refresh.

`--skip-migrate` records that database changes were skipped; it does not make
the upgrade complete. Use it only when migration is a separate known action,
then finish the migration and readiness checks before serving reads. An
installed package upgrades with `okg up` instead; see
[Back up, upgrade, restore, retire](../lifecycle.md).

## Clear stuck work

If queued work stops moving while searches still work, `okg doctor` names
the blocking work item (a workflow id). Cancel that one item:

```bash
okg runtime cancel-workflow --deployment <name> <workflow_id>
```

Do not reach for `okg runtime reset --confirm` instead: it deletes every
queued and finished work record. Use it only when a runbook says so. See
[runtime queue recovery](../../runbooks/troubleshooting/runtime-queue.md).

## When no worker is needed

Do not run a worker for:

- `okg run --once --inline --apply`, which completes one publication in the caller; or
- `okg ingest --inline`, which completes one import in the caller.
