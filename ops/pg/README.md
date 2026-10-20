# Local-dev Postgres for OKG

Brings up Postgres 17 with the four required extensions
(`timescaledb`, `pg_cron`, `pg_textsearch`, `pgvector`) and the
three substrate roles (`app_rw`, `app_ro`, `catalog_admin`). This
is the `okg pg up` target for dev.

> **Not a production target, and Aurora is not a production
> target either.** This compose is for local dev against a PG17
> instance that matches the production substrate's extension set.
> Production runs on AWS RDS, GCP Cloud SQL, Azure Database for
> PostgreSQL, or a self-hosted PG17. **Amazon Aurora Postgres is
> not supported**: it lacks TimescaleDB and `pg_textsearch`.

## Quickstart

```sh
cd ops/pg
docker compose up -d --build
export OKG_DSN=postgres://postgres:okg@localhost:5433/okg
```

The first build pulls the base image; `pg_textsearch` ships in it, and
the build fails if its bundled version differs from the one `Dockerfile`
pins. Subsequent runs reuse the image.

## Verify

```sh
docker compose exec db psql -U postgres -d okg -c \
  "SELECT extname, extversion FROM pg_extension ORDER BY extname;"
```

Expect at least these rows (plus a handful of others from the
timescaledb-ha base image):

```
    extname     |  extversion
----------------+---------------
 pg_cron        | 1.6.x
 pg_textsearch  | 1.5.1
 plpgsql        | 1.0
 timescaledb    | 2.29.0
 vector         | 0.8.x
```

`template1` carries the same set minus `pg_cron`, so a database created
later on this instance inherits the extensions:

```sh
docker compose exec db psql -U postgres -d template1 -c \
  "SELECT extname FROM pg_extension ORDER BY extname;"
```

Tests that build scratch databases with a bare `CREATE DATABASE` depend on
that: without it they fail with `access method "bm25" does not exist`
(issue #1972). `pg_cron` is absent on purpose -- PostgreSQL accepts it only
in the database `cron.database_name` names, `okg` here, and creating it
anywhere else raises `can only create extension in database okg`.

### Two things TimescaleDB in `template1` costs you

**A `trying to connect to template database` line, or two.** The TimescaleDB
background-worker launcher starts a scheduler for each database that has the
extension, and a scheduler that lands on a template database exits:

```
ERROR:  background worker "TimescaleDB Background Worker Scheduler for
database 1" trying to connect to template database, exiting
```

One line is emitted during initdb, when `01-template1-extensions.sql` creates
the extension while the launcher is already running. That one is this file's
doing. A second line has been observed about 90 seconds after the server
starts, with none after that, including after `docker restart`. **That
post-start line is not caused by this change**: it appears on an image whose
`template1` is bare as well. If you are bisecting a log, it is not the
`template1` pass.

The post-start line is host-dependent. Counting the whole log the same way on
macOS `arm64` (Docker Desktop 20.10.22, directory mount, anonymous data
volume) this image produced the initdb line and nothing else -- across 4.5
minutes of uptime and three restarts -- and the bare-`template1` image
produced no line at all. These template-database lines are harmless: a
scheduler on a template database has no jobs to run. The job-launch failures
below are not.

**Background-worker slots: 32, not PostgreSQL's default of 8.** Every
background process comes from one pool of `max_worker_processes` slots. On
this image the pool must hold:

- three launchers that run for the life of the server: the TimescaleDB
  launcher, the `pg_cron` launcher, and PostgreSQL's logical replication
  launcher (registered unless `max_logical_replication_workers` is 0). The
  replication workers themselves, up to `max_logical_replication_workers` (4),
  run only for subscriptions, which okg does not create, so they are not
  counted;
- up to `max_parallel_workers` (8) parallel query workers;
- one TimescaleDB scheduler for every database that has the extension --
  `okg`, every database `okg migrate` creates, and every scratch database
  cloned from `template1`;
- the job workers those schedulers start. TimescaleDB runs at most
  `timescaledb.max_background_workers` (16) schedulers and jobs together.

A database cloned from `template1` schedules two TimescaleDB jobs (telemetry
and job-history retention). `okg migrate` adds 22 retention and compression
policies (20 in `src/okg/substrate/db/schema.sql`, 2 in `views.sql`), so a
migrated okg database schedules **24 jobs, every one with
`max_retries = -1`**: a job that fails is retried without limit.

The worst case is therefore 3 + 8 + 16 = **27 slots**. This image used to run
with 8. Measured on a shared Linux build host with this image's old build, the
`ops/pg` compose command and five migrated okg databases: the three
launchers and five schedulers filled all 8 slots, the sixth database never got
a scheduler, and no job could start. Every launch failed with
`failed to launch job ... failed to start a background worker` -- 272 times
in the first three and a half minutes, 34,424 times in the first 19 -- and
every failure wrote a row to
`_timescaledb_internal.bgw_job_stat_history`. On a developer laptop the same
loop ran from 2026-09-30 to 2026-10-01 at about 70,000 failures an hour per
database and left about 7.5 GB of failure records (issue #3143); the
history-retention job that would trim those rows could not start either.
**These failures are not harmless.**

The image now writes `max_worker_processes = 32` into the
`postgresql.conf` of every new cluster started with `initdb/` mounted, as
every compose file here mounts it (`Dockerfile`; without the mount the base
image's own `timescaledb-tune` script sizes the setting from the host's CPUs
instead). `docker-compose.yaml` and the bench and golden overlays also pass
`-c max_worker_processes=32`, so a data volume created before the change gets
it too. `docker-compose.required-ci.yaml` does not: its settings must equal
the CI sidecar's in `ops/ci-arc/light-db/runner-values.yaml`, and it starts
every cluster on a fresh volume with `initdb/` mounted, so it gets 32 from the
image. The CI sidecar itself runs a pinned `okg-postgres` image built before
this change, so it does not get this default until that pin is moved, which
is a CI configuration change. 32 matches
`ops/environments/*/postgres-capacity.yaml`.

With 32 slots the same five migrated databases started all six schedulers,
every one of the 24 jobs in each database ran successfully, the job-history
tables stayed empty, and the log had no `failed to start a background worker`
line in the 22 minutes after the migration (proof in
`pact/changes/local-pg-worker-headroom-3143/evidence/`). During the
16 seconds of `okg migrate` itself, TimescaleDB logged 45
`failed to launch job ... out of background workers` lines while about 120
newly created jobs, all due at once, queued for its 16 workers; those wrote no
history row and stopped once the backlog had run.

More slots do not lift TimescaleDB's own cap. With about 15 or more
TimescaleDB databases on one instance, their schedulers alone fill the 16
TimescaleDB workers: `out of background workers` then never stops, jobs do
not run, and the databases beyond the 16th get no scheduler at all. Raise
`timescaledb.max_background_workers` (and `max_worker_processes` with it), or
keep fewer databases on one instance.

To fix an existing instance without recreating it, run
`ALTER SYSTEM SET max_worker_processes = 32;` as a superuser and restart the
container. On the looping instance above that stopped the failures: the sixth
scheduler started and no launch failed in the three and a half minutes after
the restart. The failed rows already written stay in each database's job
history until you delete them or TimescaleDB's history-retention job ages them
out.

`okg deployment ready <deployment>` reports the check
`database.timescaledb_workers` on every deployment. It warns, and never
blocks, when:

- `max_worker_processes` is below launchers + `max_parallel_workers` +
  `timescaledb.max_background_workers` (launchers read from
  `shared_preload_libraries` and `max_logical_replication_workers`);
- the running TimescaleDB schedulers, counted in `pg_stat_activity`, plus one
  reach `timescaledb.max_background_workers`, leaving no worker for jobs (the
  cap above; databases without TimescaleDB do not count);
- the deployment database has TimescaleDB but no running scheduler;
- the deployment database logged 10 or more failed job runs in the last hour,
  or its job history reached 128 MiB.

A part its role cannot read is reported as "unreadable" instead of a count,
and the other parts are still judged. Reading `shared_preload_libraries`
needs `pg_read_all_settings` and seeing the schedulers needs
`pg_read_all_stats`; `pg_monitor` grants both.

`timescaledb.telemetry_level` stays at its default (`basic`). Setting it to
`off` removes one of the 24 jobs per database; an earlier measurement with 8
slots cut eight scratch clones' warnings from five to one, but with 32 slots
there are no failures left to cut, so it is not needed for headroom.

## Reset

```sh
docker compose down -v
```

`-v` nukes the named volume (`okg_pgdata`) so the next `up` starts
fresh.

## Why Postgres 17, not 16

`pg_textsearch` upstream only builds for PG17 and PG18. Since that
extension is required substrate-wide, the deployment target is PG17.
Managed-host availability verification (task 0.c) must check PG17,
not PG16.

## What's in the image

- Base: `timescale/timescaledb-ha:pg17.10-ts2.29.0-all` at the immutable
  digest in `Dockerfile`, which includes
  TimescaleDB, pgvector, pg_cron, pg_stat_statements, and others.
- Added: `pg_textsearch` 1.5.1, upstream's release package
  (`pg-textsearch-v1.5.1-pg17-<arch>.zip` from the timescale/pg_textsearch
  GitHub release) installed over the base image's bundled 1.3.0, the way
  timescaledb-docker-ha installs it. The download is pinned by sha256 for
  `amd64` and `arm64` (`ARG PG_TEXTSEARCH_SHA256_*`). The build fails on a
  checksum mismatch, and afterwards unless the installed package, the
  control file's `default_version`, the install script and the library all
  name `ARG PG_TEXTSEARCH_VERSION` (the library's version is read from a
  throwaway single-user server), and unless every upgrade script from 1.3.0
  is present. The base image's own `pg_textsearch--1.3.0.sql` is removed, so
  no database can be created at 1.3.0 on the 1.5.1 library. The build
  downloads the package from github.com (`curl --retry 5`), so a GitHub
  outage fails it. 1.3.0 repeated and dropped rows when
  tied scores spanned index segments (issue #3335) and stopped one index
  scan at 100,000 results (issue #3162).

## Upgrading an existing database to pg_textsearch 1.5.1

A new image replaces the library, not the extension's SQL objects.

**The move cannot simply be rolled back. Take a backup of each database
first.** Two things change on the way, and the 1.3.0 library understands
neither:

- the first write to a BM25 index under 1.5.1 rewrites its header from
  version 7 to 9; the 1.3.0 library then refuses the index with
  `incompatible pg_textsearch index version`, so going back needs a
  `REINDEX` of every BM25 index under 1.3.0, or a restore;
- after `ALTER EXTENSION pg_textsearch UPDATE`, the catalog names C
  functions (`bm25_compact` and others) that the 1.3.0 library lacks.

After a database's server runs this image, once per database (as a
superuser):

```sql
ALTER EXTENSION pg_textsearch UPDATE;   -- 1.3.0 -> 1.5.1
SELECT extversion FROM pg_extension WHERE extname = 'pg_textsearch';
```

The upgrade scripts add functions (with EXECUTE revoked from PUBLIC on the
maintenance and test ones) and one operator, `@@ (text, tsquery)`, to the
BM25 operator family. 1.4.0's script also marks the two score functions
`bm25_text_bm25query_score` and `bm25_textarray_bm25query_score` STABLE
instead of IMMUTABLE, so the planner no longer folds them at plan time,
which can change plans. 1.4.0's library also requires SELECT on the indexed
table for a standalone `<@>` score. No BM25 index needs a rebuild. 1.5.1 reads 1.3.0's index header (version 7) and rewrites
it to version 9 on the index's first write. Measured on a small copy (a
200,000-row table and a 1,200-row table indexed on 1.3.0): queries under
the 1.5.1 library returned the no-index statement's rows before and after
`ALTER EXTENSION`, and after a write; `ALTER EXTENSION` took 14 ms.

`REINDEX` is optional. It compacts an index that 1.3.0 built in several
segments (on that copy, 21 MB to 3.4 MB in 2.5 s for the 200,000-row
table), and upstream recommends it for fragmented indexes. Time it on a
copy of the database first: it holds a lock that blocks writes to the
table. A comp-ops-sized timing is a follow-up (issue #3335).

## Why not `postgres:17` + install everything

Shorter path, fewer moving parts. The `timescaledb-ha` image is the
upstream-recommended way to run TimescaleDB and handles three of the
four extensions for us. Building everything from the stock
`postgres:17` image adds Timescale + pgvector + pg_cron build steps,
each a source of fragility; we'd be reinventing what the HA image
already does.

## Safe development database clones

Do not use `CREATE DATABASE ... TEMPLATE <live_database>` and do not terminate
database backends before a reset. The supported development path is:

```sh
OKG_CLONE_SOURCE_DSN="$SOURCE_DSN" \
OKG_CLONE_TARGET_DSN="$TARGET_DSN" \
okg db clone --apply --json
```

The command makes a custom-format dump, stops TimescaleDB work through its
documented restore hooks, blocks and drains target sessions without signaling
them, recreates from `template0`, excludes server-bound pg_cron state, and
restores with the source's exact TimescaleDB version. Source, target, and an
optional `OKG_CLONE_ADMIN_DSN` are resolved to PostgreSQL system identifiers;
service/multi-host connection options are pinned before `pg_dump`/`pg_restore`
runs. It refuses a target equal to `cron.database_name`; use a separate
disposable database name. Existing target ownership, connection state, and
database privileges are restored only after the clone verifies cleanly.

## License note

We use only Apache-2 features of TimescaleDB (hypertables,
retention, compression) and nothing that requires TSL. The HA image
is convenient; the substrate does not depend on TSL code paths.
