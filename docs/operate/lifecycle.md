---
audience: operator
type: how-to
canonical_id: task-backup-restore-retire
section: operations
visibility: primary
status: active
owner: documentation
---
# Back up, upgrade, restore, or retire a deployment

These actions can make data unavailable or permanently remove it. Confirm the
exact deployment, database, owner, approval, and recovery point before acting.

## Back up

Verify the declared deployment database with `prod`. Use `dev` only for a
worktree-isolated database:

```bash
okg deployment env <deployment> --json
okg deployment backup verify <deployment> --target prod --json
```

The verification must prove the archive can be restored. A file on disk is not
enough.

## Upgrade

How you upgrade depends on where the deployment's OKG code comes from:

- **An installed okg package.** `okg install` makes this kind: the deployment
  declares `release.kind: standalone`, or OKG runs from a wheel
  (`release.kind: distribution`, declared or detected). Upgrade the package,
  then run `okg up <deployment>`. See
  [Upgrade an installed deployment](#upgrade-an-installed-deployment).
- **A Git checkout of okg.** `okg up <deployment>` syncs the deployment to its
  pinned code revision and runs the steps below.

For a Git-checkout deployment:

1. Confirm its identity, profile, database, and release.
2. Create and verify a recovery archive.
3. Read the release-specific upgrade notes.
4. Apply the upgrade through that deployment's documented runtime procedure.
5. Check readiness, the latest published version, and database health.

Follow [Back up](#back-up) to confirm the deployment identity and
verify a restorable recovery archive before applying the upgrade.

After the upgrade:

```bash
okg deployment ready <deployment> --checks general --json
okg generation list --deployment <deployment> --json
okg doctor --deployment <deployment> --json
```

A non-zero migration, readiness, or doctor result blocks the upgrade. Keep the
error code and deployment identity, then follow
[Diagnose and recover](../runbooks/index.md). Do not switch to a different
database or profile merely to make the check pass.

### Upgrade an installed deployment

Install the newer okg package first, the same way you installed okg, then run
the same `okg up`. Each release's wheel is attached to its
[GitHub release](https://github.com/mitdbg/akmon/releases):

```bash
uv tool install --force okg-<version>-py3-none-any.whl   # or: pip install --upgrade <the same wheel>
okg up <deployment> --plan --json   # checks compatibility; changes nothing
okg up <deployment> --json
```

`okg up` needs `pg_dump` and `pg_restore` at the database's major version or
newer; see [Install OKG](install.md#before-you-start). Stop `okg mcp-serve`
for the deployment yourself before you start: `okg up` stops only the
runtime worker it can find.

`okg up` runs these steps in order and stops at the first failure:

1. **Compatibility.** It checks the deployment against the OKG release now
   installed, as `okg deployment compat` does, and lists the breaking changes
   since the release you upgraded from. A blocker, or a breaking change that
   touches something this deployment uses, refuses with
   `installed_upgrade_incompatible` before anything is changed. The message
   lists each row, links the release notes when it knows them, and names the
   version to reinstall to go back.
2. **Preflight.** It records which database it is about to change (the
   server's identifier, the database name and port), the published graph
   readers see, and the findings `okg deployment ready` already reports, and
   checks that `pg_dump` and `pg_restore` are present and new enough. The
   database is the one the deployment's `deployment.yaml` names; `--dsn` is
   refused. Every `okg` command the upgrade starts runs against that same
   database, and the later steps check that it has not changed.
3. **Stop the worker.** When a runtime worker for the deployment is running,
   `okg up` stops it with `okg dogfood down`, checks that it is gone, and
   starts it again in step 7 through the deployment's own worker service;
   step 6 reloads the worker through it too. On Linux that service is the
   systemd system unit: the instance `okg-runtime-worker@<deployment>.service`
   must be enabled, and the shared template
   `/etc/systemd/system/okg-runtime-worker@.service` must be exactly the one
   OKG renders for this deployment (another deployment's template does not
   count). On macOS it is the launchd plist
   `~/Library/LaunchAgents/org.okg.runtime-worker.<deployment>.plist`, which
   the reload writes itself, without root.

   `okg up` refuses right after the compatibility check, also with
   `--plan`, with `installed_upgrade_worker_unsupervised`, and changes
   nothing (no stop, no backup, no unfinished-upgrade marker), when:

   - The deployment has a systemd system worker service, the reload is
     needed (after any change of the OKG release once a worker has run), and
     `okg up` does not run as root. The reload rewrites the system unit, so
     **`okg up` for a deployment with a systemd system worker service must
     run as root** (for example with sudo).
   - On Linux, the deployment has no worker service of its own and either
     has `runtime.enabled: true`, has a worker running, or has run a worker
     before (its record of the code it ran, the ack, would need a reload).
     Stopping a running worker does not help: the ack stays. Install the
     service as root first (`okg dogfood service install --deployment
     <deployment> --adapter systemd --apply`), then run `okg up` as root.
   - On macOS, a worker is running with no plist yet. Install the plist
     (`okg dogfood service install --deployment <deployment> --adapter
     launchd --apply`, no root needed) and run `okg up` again. Unproven on a
     Mac so far; the macOS path is covered by unit tests only.
   - Whether the reload is needed cannot be read (the apply health check is
     unavailable): `okg up` fails closed and names the error.

   `okg up` decides about the reload with the same read-only check
   `okg deployment apply` uses. On a Linux host without root, a deployment
   that has ever run a worker cannot use `okg up` until per-user systemd
   worker services land, the first follow-up after 2026-10-17. A
   deployment that has never run a worker, with `runtime.enabled: false`,
   upgrades without a service. When an earlier `okg up` did not finish, the
   refusal says what that run already changed instead of "nothing was
   changed".
4. **Back up.** `pg_dump` writes the database, with its owners and
   privileges, into the deployment's private state directory (below), or
   into `--backup-dir`. A `--backup-dir` inside a Git work tree is refused
   unless that work tree ignores it. The archive is restored into a
   disposable database on the same server, its owners, grants and default
   privileges are replayed, and its migrations, catalog versions, published
   generations, owners and privileges are compared with the original
   (TimescaleDB's chunks take their hypertable's owner and grants and are
   compared through it, because a restored copy runs TimescaleDB's
   compression and retention jobs at once, which creates and drops chunks
   on its own; a job owned by a role other than a superuser may record one
   failed run in `timescaledb_information.job_errors` in that copy, which is
   harmless and retried on its schedule). When
   they match, the disposable database is dropped and a sidecar,
   `<archive>.json`, records what the archive holds. When they do not, the
   upgrade stops and the disposable database is kept and named in the
   report, so you can look at it; drop it when done. Archives are kept;
   delete old ones yourself.

   This check, like the restore below, needs a superuser when the database
   uses TimescaleDB (its restore hooks set a superuser parameter, and the
   restore has to end TimescaleDB's superuser-owned scheduler). Without
   TimescaleDB a role with CREATEDB and membership in the database owner's
   role is enough, but its replay of grants fails on any grant made by a role
   other than the object's owner. Step 2 checks this before anything changes
   and refuses otherwise.
5. **Migrate.** `okg migrate --apply`, then a check that this database has no
   migration left pending.
6. **Converge.** `okg deployment apply`, when the deployment is not already
   converged (catalog, Nomos baseline, sources, worker configuration).
7. **Restart the worker**, when this upgrade stopped it.
8. **Readiness.** `okg deployment ready` must report no blocker and no
   unknown finding that it did not report before the upgrade. A finding is
   compared by its check, status, message and cause, so a check whose cause
   changed counts as new. Findings that already existed are listed, never
   hidden. The generation readers saw before must still be published with
   the same node and edge counts.
9. **Receipt.** It writes the installed-release receipt (below): the OKG
   version, the installed wheel's digest, the schema it ships, OKG's public
   surface (its release contract), the previous version, the backup archive
   and the times. `okg install` writes the same receipt, so the next upgrade
   knows what it is upgrading from.

**Where things are kept.** Everything `okg up` keeps for a deployment is in
`${OKG_USER_HOME:-$XDG_DATA_HOME/okg}/state/deployments/<deployment>/`
(`~/.local/share/okg/...` by default), readable only by you, outside the
deployment's own directory: `installed-release.json`, the `backups/` archives
with their sidecars, `restores/` records, and `upgrade-in-progress.json` while
an upgrade is unfinished.

**Running it again.** When the receipt already names the installed okg and
the database has no pending migration, `okg up` reports `up_to_date` and
changes nothing. A run that fails is finished by fixing the cause and running
`okg up` again: every step can run twice, the receipt is written only after
all of them pass, and `upgrade-in-progress.json` carries into the next run
whether the worker was stopped (so it is restarted) and which archive was
taken before the upgrade began. An OKG release older than the receipt's is refused
with `installed_upgrade_refused`: going back is a restore, below. Options that
only mean something to a Git checkout, such as `--skip-backup`, `--target dev`
or `--state-file`, are refused rather than ignored. A damaged receipt, or one
written for another deployment directory, is refused with
`installed_release_receipt_unreadable`. `okg install` does not overwrite a
receipt that names a different okg; it keeps it and prints a warning.

**No receipt yet.** A deployment installed before the receipt existed has
none, so the release it came from is unknown and `okg up` refuses with
`installed_upgrade_compat_unknown`. Give it the previous release's contract.
Before upgrading the package, write it with the okg that is still installed:

```bash
python -c 'from okg.substrate.release_contract import build_contract, render_contract; print(render_contract(build_contract()), end="")' > okg-contract-previous.json
```

Run it with the Python of the environment okg is installed in. If you have
already upgraded, install the previous wheel into a scratch environment and
run it there, or, from a checkout of that release, run
`scripts/release/build_contract.py` with `--out FILE`. Then:

```bash
okg up <deployment> --from-contract okg-contract-previous.json --json
```

`--from-contract` also works when a receipt exists: `okg up` then warns that
it overrides the receipt and records that in the new receipt. A contract
identical to the installed okg's is refused, because there is nothing to
compare. `--accept-unknown-compat` upgrades without the check. `okg up`
prints a warning and records the choice in the receipt. It never overrides a
blocker.

**Going back.** Before the migration step, reinstall the previous version's
wheel the way you installed it (`uv tool install --force
okg-<version>-py3-none-any.whl`) and start the worker again if it was
stopped (`okg dogfood up --deployment <deployment> --apply`). After the
migration, the database has to go back too. Do that with the okg that is
installed now, the one that took the backup, before you reinstall the old
wheel:

```bash
okg deployment backup restore <deployment> --archive <archive> --json          # the plan; changes nothing
okg deployment backup restore <deployment> --archive <archive> --apply --json
```

`<archive>` is the `pre_upgrade_archive` in the `okg up` report, in
`upgrade-in-progress.json`, or in the receipt's `upgrade` section; the
restore refuses any other archive, because a backup taken by a re-run of a
failed upgrade can already hold part of the upgrade. It also refuses an
archive without its `<archive>.json` sidecar, a sidecar that other users can
read or that you do not own, an archive whose bytes changed, one taken from
another database, and a role without the rights named in step 4. With
`--apply` it stops the worker, restores the archive into a fresh database on
the same server, replays the archive's owners, grants, PUBLIC revokes and
default privileges, and checks both the data and the owners and privileges
against the sidecar's record. It then gives the new database the live
database's owner, grants and settings, and swaps it in by renaming: the live
database is kept as `<database>_pre_restore_<time>`, and nothing is dropped.
It refuses while any other session stays connected to the live database
(stop `okg mcp-serve` and other clients first); it ends only TimescaleDB's
own background workers, starts them again after the swap, and reopens both
databases whatever happens. It puts back the receipt the deployment had
before the upgrade and moves the newer receipt and the unfinished-upgrade
marker aside right after the swap, so a check that fails later still leaves
them describing the restored database. It does not restart the worker; its `next_action` says so when
the upgrade or the restore stopped it. Then reinstall the previous wheel,
check with `okg deployment ready <deployment>`, and start the worker.

To restore an archive into a separate database instead, without touching the
deployment, use `okg db clone --restore-dump`. Never put a database password
on a command line: `pg_dump` and `pg_restore` read it from `PGPASSWORD` or a
`PGPASSFILE`.

A deployment pinned with `release.channel: prod` names its okg in
`release.pin`; update the pin to the new values (`okg deployment release
<deployment> --json`) before the upgrade, or readiness refuses with
`release_pin_mismatch`.

### Upgrade to 0.2.0

Migrate every existing database once after installing 0.2.0. The package compares a
checksum of the whole schema text, comments included, with the one recorded
in the database, so the two differ even when the only schema change is SQL
comments. When the deployment declares a `release:` block, `okg runtime
worker` and `okg mcp-serve` refuse to start with `schema_checksum_mismatch`;
without one they start, and `okg doctor` reports
`schema_migration_checksum_mismatch`. When the difference is only comments,
the migration leaves every node and edge count unchanged.

The migration is one-way: afterwards an install whose schema differs from
0.2.0's reports the same mismatch against the database. To go back, restore
the backup taken before the upgrade.

- A deployment `okg up` manages: follow the steps above. The recovery archive
  is the backup, and `okg up <deployment>` runs the migration.
- An installed deployment: follow
  [Upgrade an installed deployment](#upgrade-an-installed-deployment). Its
  migration step is the migration.
- A deployment with `release.channel: prod`: its `release.pin` names the old
  code revision and schema checksum, so after migrating it still refuses to
  start, with `release_pin_mismatch`. Update `release.pin` in its
  `deployment.yaml` to the 0.2.0 values before restarting, and check with
  `okg deployment release <deployment> --json`.

#### Move search to pg_textsearch 1.5.1

The 0.2.0 okg PostgreSQL image (`ops/pg`) ships pg_textsearch 1.5.1, the
extension behind keyword search. Version 1.3.0 can list a search result twice
and leave another out when scores tie, and it stops after 100,000 index
results. `okg up`, `okg migrate` and the upgrade steps above do not change
the extension, so an existing database keeps 1.3.0 until you move it. A new
install on the 0.2.0 image starts on 1.5.1 and needs none of these steps.
While a database runs a version below 1.5.1, `okg doctor` lists the warning
`extension_outdated`, with the installed version and this section; the doctor
still passes.

The move is one-way. After the first write under 1.5.1, each BM25 index is
stored in a format 1.3.0 refuses to read. Before step 3, going back takes a
`REINDEX` of every BM25 index under 1.3.0; that path has not been tested.
After step 3, only a restore works: the database then names functions that
1.3.0 does not have, and the extension ships no scripts to step back down.

1.  Back up the database, as in [Back up](#back-up).
2.  Restart the database server on the 0.2.0 image. Run these from a 0.2.0
    checkout. The repeated results stop as soon as the server runs the new
    image.

    -   The `ops/pg` compose file, the academic-researcher profile and other
        local setups, such as per-deployment stacks, run the local image
        `okg-pg17:local`, and `okg install` builds it only when it is
        missing. Rebuild it, then recreate each server that uses it.

        `okg install` lets Docker pick an academic-researcher deployment's
        host port, so read the current one first and pass it back, or the
        recreated server moves to a new port. Leaving out
        `POSTGRES_PASSWORD` is harmless: an existing data volume ignores it.

        ```bash
        docker compose -f ops/pg/docker-compose.yaml build
        # the ops/pg database
        docker compose -f ops/pg/docker-compose.yaml up -d
        # an academic-researcher deployment's database
        docker port okg-<deployment>-postgres 5432
        DEPLOYMENT_NAME=<deployment> POSTGRES_PORT=<port> docker compose \
          -f src/okg/substrate/library/profiles/academic-researcher/compose.yaml \
          -p okg-<deployment> up -d --wait
        ```

    -   Helm chart: build `ops/pg` at the 0.2.0 commit and push it to your
        registry under a new tag, then set `postgresql.image.tag` to that
        tag, or set `postgresql.image.digest`. Never push over the old tag:
        the chart's `pullPolicy` is `IfNotPresent`, so a node that already
        has the old tag keeps running it. Then run the same
        `helm upgrade --install` as in
        [Deploy an OKG on Kubernetes](deployments/kubernetes.md#install).

3.  In each database, as a superuser, update the extension and check its
    version. On a small copy the update took about 15 ms. Connect with, for
    example:

    ```bash
    docker compose -f ops/pg/docker-compose.yaml exec db psql -U postgres -d okg
    docker exec -it okg-<deployment>-postgres psql -U postgres -d okg_<deployment>
    kubectl -n <namespace> exec -it statefulset/<prefix>-postgres \
      -c postgres -- psql -U postgres -d okg
    ```

    On Kubernetes, `<prefix>` is defined in
    [Kubernetes](deployments/kubernetes.md#credentials-and-the-database-roles).

    Then run:

    ```sql
    ALTER EXTENSION pg_textsearch UPDATE;
    SELECT extversion FROM pg_extension WHERE extname = 'pg_textsearch';
    ```

    The query must return `1.5.1`. If the server still runs the old image,
    the update does not fail: it only notes that 1.3.0 is already installed.
    The version check is what tells you the move happened.

4.  Optional: rebuild each BM25 index to compact the parts 1.3.0 left.
    Search gives the same results without it. A rebuild locks its table
    against both reads and writes while it runs: 2.7 s for 200,000 rows on a
    test copy. Time it on a copy first. This query prints one
    `REINDEX` command per BM25 index; run each one:

    ```sql
    SELECT format('REINDEX INDEX %I.%I;', n.nspname, c.relname)
    FROM pg_class c
    JOIN pg_am am ON am.oid = c.relam
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'i' AND am.amname = 'bm25';
    ```

### First migrate after search grep is added

The first `okg migrate` (or `okg up`) after upgrading to a release with
`search(method="grep")` builds the grep text index and blocks writes to the
graph while it builds, about 20 seconds per 100,000 records with text (42 s
for 195,000 on a copy of a production graph, on a busy host); plan that pause.
The release that adds grep's `path:` term also builds a trigram index on
record paths: under a second and about 5 MB per 100,000 paths, and about
15 ms more write time per 1,000 new records.

### Restore record bodies to search after upgrading

Before this change, when a record type listed searchable fields and one of
them had a value, AKMON rebuilt the record's search text from those fields
and dropped the body its source had written into `text`. Search could not find a word that appeared only in the body. New
imports now keep the body: it is searched after the other fields, and a copy
is stored in the system attribute `okg_producer_text`. MCP tool responses, the
operator console's record and neighbour reads, and the Nomos node read leave
that attribute out, and a redacted read withholds it exactly as it withholds
`text`. Direct SQL against the `okg.graph_nodes` table or the `okg.v_*` views
still shows it.

A body that was already dropped is gone from the stored record, so migrating
and reloading the catalog cannot bring it back. Only reading the source again
does. After upgrading the package:

```bash
okg migrate --deployment <deployment> --apply --json
okg catalog load --deployment <deployment-dir> --apply --json
okg ingest --deployment <deployment> --inline --reset-cursor --include <source> --json
```

`--reset-cursor` makes the source read its whole scope again, ignoring its
saved progress. `--inline` runs that repair in this process; it is required
when the deployment's runtime is enabled (`okg ingest` refuses a queued
`--reset-cursor`), and harmless when it is not, since an ingest then runs in
this process anyway. Only records whose search text changes are rewritten, and the
import publishes a new version when it finishes. Run it for each source whose
records carry a body in `text` on a type that also lists other searchable
fields. The known ones are:

- An `analysis_notes` source that imports one record per document section: each section's text read as its file name, such as
  `03-Selection`.
- Sources created by `okg deployment bootstrap`: each record read as its title
  and each finding as its label. A deployment drafted before this change keeps
  its `title,text` and `label,text` declarations; the re-read restores the
  body, because the bootstrap source writes it into `text`. A deployment
  drafted after this change declares `title,body` and `label,body` and writes
  the body into `body`.
- OpenSpec reasoning records (`Hypothesis`, `Observation`, `Insight`).

Every rewritten record is stored with no embedding. Where embeddings are
enabled, the runtime worker's embedding job embeds them again on its schedule;
start the worker with `okg start --deployment <deployment>` if it is not
running. Each rewritten record costs one embedding call.

The re-read does not restore Markdown headings (`markdown_heading`, from the
code-structure enricher), whose search text is the heading's slug instead of
its words. That enricher parses only files that have no heading records yet
(`library/enrichers/code_graph/_structural.py`), and it does not re-parse a
file when its own version changes. No command re-parses them today, so those
headings keep the slug as their search text until their heading records are
removed.

### Shorter search text for records whose body repeats their fields

From this release, a record's search text leaves out a searchable field whose
value already appears as a whole word in the body its source wrote. Before, the
field came first and the body repeated it, so the search text of some types
was about twice as long as its content (a Jira issue repeated its key, summary,
status and priority). Words split out of path fields and CamelCase names are
still added.

Stored records keep their old search text until something rewrites them: a
source sends the record again, a catalog change touches its type, or a re-read
with `--reset-cursor` as above. Each rewritten record gets a new text hash, so
it is embedded again, at one embedding call per record. On a large graph
that can be several hundred thousand records (issues, documentation pages, code symbols,
references and imports, releases, endpoints and repositories). Keyword ranking
also shifts for those types, because their text is shorter.

## Restore

To return to the backup `okg up` took, use `okg deployment backup restore`
as described under [Upgrade an installed deployment](#upgrade-an-installed-deployment),
"Going back". Stop the [background worker](deployments/runtime.md) and other
writers first. Restoring any other backup has no `okg` command yet.

After a restore, run deployment readiness, `doctor`, list published versions,
and search pinned to the intended recovered version.

## Retire

Before retirement, record:

- the final published graph version;
- all connected sources and credentials to revoke;
- the approved retention, export, or deletion decision;
- the verified recovery archive, if retention requires one; and
- the person responsible for completing and auditing the retirement.

Stop scheduled imports and background work through the deployment's runtime
procedure. Revoke source access only after required exports and archives are
verified. Database deletion must follow the
[destructive-drop procedure](deployments/database.md#destructive-drops),
which requires an explicit database target, confirmation, and apply flag.

Stop if the database identity is unclear, approval is missing, a backup cannot
be verified, or post-restore checks fail.
