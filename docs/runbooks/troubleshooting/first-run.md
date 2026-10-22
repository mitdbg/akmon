---
audience: developer, operator
type: how-to
canonical_id: runbook-first-run-triage
---
# First-Run Triage


Use this runbook when a fresh deployment's first bring-up does not end in a
searchable graph. Each section names the failure signature, the emitting
surface, the cause, and the fix.

## 0. The bundled database will not start: port 5433 or container okg-pg in use

- **Signature:** `docker compose up` in `ops/pg` stops with an error that
  `127.0.0.1:5433` is already allocated or in use.
- **Cause:** another program holds the port. Find it with
  `docker ps --filter publish=5433`; if nothing is listed, it is not a
  container, often a local PostgreSQL.
- **Fix:** if it is another graph's AKMON database, start this one as a
  second graph, with its own project, container and port, as the
  [Quickstart](../../start/quickstart.md#1-get-akmon-and-start-the-database)
  shows. Otherwise start it on a free port, for example
  `OKG_PG_PORT=5434 docker compose up -d --build --wait`, and use that port
  in the connection string.
- **`container name "/okg-pg" is already in use`:** a database from an older
  AKMON, which ran in Compose project `pg` (now `okg-pg`). To keep it, set
  `COMPOSE_PROJECT_NAME=pg` for every compose command, if no other app here
  uses a project named `pg`; its data is in the volume `pg_okg_pgdata`. If
  only that volume is left, `up` starts an empty database instead; the same
  setting reaches the old data.

## 0b. `podman run` stops with `crun: join keyctl: Disk quota exceeded`

- **Signature:** starting the database container with rootless podman fails
  with `OCI runtime error: crun: join keyctl ...: Disk quota exceeded`.
- **Cause:** on a shared login node, each running container uses one kernel
  key, and your user has reached its key limit (see `/proc/key-users`). The
  database image is not at fault.
- **Fix (podman only):** turn the kernel key off for every container you run.
  In `~/.config/containers/containers.conf`, add:

  ```toml
  [containers]
  keyring = false
  ```

  Then run `podman run` again. (`--annotation run.oci.keyring=false` on the
  command line does not help.) Removing containers you no longer need frees
  keys too, and an administrator can raise the kernel limit
  `kernel.keys.maxkeys`.

## 1. Manifest fails to resolve: `points at a missing path`

```
DeploymentConfigError: deployment.yaml required key 'schema_dir' points at
a missing path: .../deployments/<deployment>/schemas
```

- **Emitted by:** every command that loads the manifest.
- **Cause:** the deployment directory is incomplete. Current `okg install`
  scaffolds include a base schema file under `schemas/`; an older scaffold,
  or a copy made by a tool that drops empty directories, can lack it.
- **Fix:** re-run `okg install --profile ...` with a current build, or create
  the directory with a minimal base schema file and re-run. A freshly
  installed deployment hitting this is a bug: file it with
  `okg deployment bug-report`.

## 2. Database does not exist

```
database '<db-name>' does not exist on the target server
fix_hint: create it, then re-run: `createdb -h <host> -p <port> -U <user> <db-name>`
```

- **Emitted by:** `okg provision` / `okg install` on first database contact
  (SQLSTATE `3D000` or the connect-phase FATAL message).
- **Cause:** the DSN names a database nobody created; install and provision
  never create one.
- **Fix:** run the `createdb` command from the error (prefix
  `PGPASSWORD=okg` for the bundled stack), then re-run. A raw psycopg
  `OperationalError` instead of the hint means an old build.

## 2b. Required extension missing: `ExtensionMissing`

```
ExtensionMissing: required extension '<name>' is not installed
```

- **Emitted by:** `okg migrate` against a database whose extensions were
  never provisioned. Extensions are per-database: the bundled stack's initdb
  installs them only into its initial `okg` database, so a fresh `createdb`
  starts bare.
- **Cause:** migrate ran without ensuring extensions. Current `okg provision`
  and `okg install` provision the required set first; this means an old
  build or a hand-run `okg migrate`.
- **Fix:** re-run `okg provision --deployment <deployment>` (or `okg install`),
  or by hand `psql -d <db-name> -c 'CREATE EXTENSION IF NOT EXISTS "<name>"'`
  for each named extension, then re-run migrate.

## 3. Every source FAILs: `nomos.default_deny.no_active_bundle`

```
'status': 'FAIL', 'error': 'SubstrateError: Nomos denied source.ingest:
 nomos.default_deny.no_active_bundle'
```

- **Emitted by:** `okg ingest --progress` per source; `okg provision`
  lists the failing sources and distinct reason codes in its summary.
- **Cause:** the deployment enforces Nomos at runtime but no policy bundle
  is active: the baseline seed never ran (older build) or the bundle was
  deactivated.
- **Fix:** re-run `okg provision --deployment <deployment>` on a current
  build, or `okg deployment up <deployment>` (both run the idempotent
  baseline seed), then re-run ingest.

## 4. Publish proof failed: empty or unpublished generation

```
provision: ok=false ... publish_proof: {'ok': False, ...}
# or: latest_published_status != 'published', latest_published_node_count == 0
```

- **Emitted by:** `okg provision` (exits nonzero) and `okg --json status`.
- **Cause:** every source failed or emitted nothing (missing optional
  dependencies, signature 5; Nomos denial, signature 3; a wrong scope root;
  an adapter import failure), or the publish itself was refused (signature
  6c).
- **Fix:** read the per-source reasons in the provision summary, or re-run
  `okg ingest --deployment <deployment> --progress` and fix the first failing
  source. `okg doctor --check sources` reports adapter import/config failures
  directly.

## 5. `TreeSitterNotInstalled` (or another missing optional dependency)

```
TreeSitterNotInstalled: tree-sitter-language-pack not installed; required
for the `<language>` code extractor [running interpreter:
/Users/<you>/.local/share/uv/tools/okg/bin/python; uv tool environment]
```

- **Emitted by:** extras-requiring sources (`code_structure`, quality
  ingest) at run time.
- **Cause:** the interpreter that ran the command lacks the extras. A path
  under `~/.local/share/uv/tools/` means the global tool shim ran, not the
  checkout venv, so `uv run --extra ...` never reached it.
- **Fix:** from a checkout, `uv run --extra code --extra quality okg ...`
  and confirm `uv run which okg` resolves inside `.venv`; for a global tool,
  `uv tool install "okg[code,quality]"`. `okg deployment env doctor
  <deployment>` reports the running interpreter.

## 5b. `TreeSitterNotInstalled`: the grammar cache cannot be used

```
TreeSitterNotInstalled: tree-sitter grammar for 'go' could not be loaded: the
grammar cache <path> cannot be used; set TREE_SITTER_LANGUAGE_PACK_CACHE_DIR
to move it (Download error: Failed to create cache directory ...: Permission
denied (os error 13)) [running interpreter: <python>; project venv]
```

- **Emitted by:** every code-structure source when it is built, because each
  one loads all tree-sitter grammars.
- **Cause:** grammars download on first use into a per-user cache under the
  home directory, and that cache cannot be created or written (or is group-
  or other-writable, which the grammar package refuses).
- **Fix:** export `TREE_SITTER_LANGUAGE_PACK_CACHE_DIR=<dir>`, a directory
  this user owns and can write, and rerun the ingest. If the message says
  the variable is already set, point it somewhere usable. On Linux,
  `XDG_CACHE_HOME` also moves the cache while the variable is unset. The
  first download needs network access.

## 5c. `TreeSitterNotInstalled`: the grammar download timed out

```
TreeSitterNotInstalled: tree-sitter grammar for 'c' could not be loaded:
Download error: Failed to read download body: timeout: global
```

- **Emitted by:** every code-structure source, on its first run.
- **Cause:** the grammars (about 500 MB) download on first use, and the
  download gives up after about a minute; no setting raises the limit.
- **Effect:** the other sources still publish; code-structure records, and
  the records built from them, are missing.
- **Fix:** download the grammars where the network is fast, into a folder
  you then reuse, and rerun the command that failed (for `okg install`, add
  `--force --yes`) with the same variable set:

```bash
export TREE_SITTER_LANGUAGE_PACK_CACHE_DIR=<dir>
python -c 'import tree_sitter_language_pack as t; t.download_all()'
```

## 6. Generation `blocked`

- **Emitted by:** `okg --json status` (`latest_published_status:
  blocked`) and `okg generation describe <generation-id>`.
- **Cause:** a Phase B invariant failed or timed out; the publish was
  refused.
- **Fix:** follow [generation-blocked](generation-blocked.md); it maps
  `last_blocked_reason` to actions.

## 6b. Upgrade stops at `backup`: `effect_inventory` is `unreadable`

```
effect_inventory: {'status': 'unreadable',
  'error': {'type': 'ValueError', 'message': 'target Git tree contains ...'}}
failed_phase: backup
reason: physical rollback admission refused mutation
```

- **Emitted by:** `okg deployment up`. Before it touches the database the
  planner projects the **committed** tree into a temporary candidate tree so it
  can compose and fingerprint the catalog the target revision would apply. An
  unreadable projection refuses the backup admission, so the run stops at
  phase 2 of 14 — no migrate, no catalog apply, no publish, no readiness.
- **Cause:** something in the deployment directory or in
  `src/okg/substrate/library/ontologies` cannot be projected faithfully. A
  tracked symlink whose target resolves inside the same repository is **not**
  a cause — those are followed and replaced by their target's content. Every
  refusal below names the offending member and a remedy:
  - a symlink **outside the repository** — an absolute target, or one climbing
    above the repository root;
  - a symlink that **points at itself or its own parent**, which would copy its
    own container into itself;
  - a symlink whose target is **not tracked at the planned revision**;
  - a symlink **chain longer than** the seven links the planner follows;
  - a symlink that **does not resolve to a tracked path** — most often a cycle
    (`a` points at `b`, `b` points back at `a`);
  - a symlink that **lands on a path the candidate tree already holds**, or one
    that **crosses repository authority** by reaching into the other repository
    of a vendored layout;
  - a member that is **neither a file, a directory, nor a symlink** (a device
    node or FIFO), or one resolving to something that is
    **neither a file nor a directory**.
- **Fix:** follow the message — commit the link's target, or replace the
  link with the file it points at, and **commit the fix**. The projection reads
  the committed tree, so changing only the working tree does nothing and makes a
  correct diagnosis look wrong. Re-run `okg deployment up` and confirm
  `effect_inventory.status` is no longer `unreadable`.
- **If the message calls itself a planner bug** (an unresolved symlink survived
  the projection, or closure bookkeeping disagreed with itself), nothing in your
  tree will fix it — file it with `okg deployment bug-report`.

## 6c. Publish refused; the next import says `staged source work was not published`

```
staged source work was not published; refusing to start new source writes
publish_code: <cause>  publish_error: ...  fix_hint: <the publish's own repair>
```

- **Emitted by:** `okg ingest` and `okg provision` before any source write:
  the previous run imported records but their publish was refused. Readers
  stay on the last published generation.
- **Fix:** keep the deployment and database. Fix the cause the hint names,
  then re-run `okg ingest --deployment <deployment> --json`: it publishes the
  staged records once; a repeat reports `no_source_work`. A deployment that
  never published and holds nothing staged is re-run with `okg provision`.
- **Not the fix:** `okg deployment up`. It upgrades a published deployment;
  here it stops at `backup`, and its `publish_recovery` field names the
  command above.
- **After `okg install --force` over an existing database**, the install
  exits 1 at `ingest` with `catalog_manifest_drifted`: the final publish was
  refused because the database still holds the old claimed files, and
  `okg status` shows what was published. Claim the new files, then publish:
  `okg catalog ownership claim --deployment <deployment>`, then
  `okg run --once --deployment <deployment> --apply`.

## 7. Search denied: `nomos.default_deny` on `graph.read.cli_search`

- **Emitted by:** `okg search` under an enforce-mode deployment.
- **Cause:** the caller's subject is not granted read. Operator CLI reads
  present the manifest's operator read principal by default; agents must
  set `OKG_NOMOS_SUBJECT=subject:agent`. A subject outside `read_principals`
  is denied by design.
- **Fix:** for agents, export the agent subject; for operators this should
  not occur: check `okg deployment env <deployment> --json` identity and
  file a bug.

## 8. Import succeeded but one record is missing

Importing and publishing are separate. Confirm the published generation and
search that exact version before importing again:

```bash
uv run okg generation list --deployment <deployment> --json
uv run okg generation describe <generation-id> \
  --deployment <deployment> --json
uv run okg search --deployment <deployment> \
  --query "<known text or record id>" --method lexical --limit 10 \
  --generation-id <generation-id> --json
```

The generation must report `published` and the search result must name the
same `generation_id`. If the record is absent, check in order: the source read
the expected revision, admission accepted the record, publication completed,
the search is pinned to that publication, the caller may read the record, and
the query matches a stored field. Route a blocked publication to
[Publication keeps failing](generation-blocked.md) and a stale source to
[A source stopped updating](source-sync.md). Do not rerun the import until the
failed stage is known. Recovery requires a newly published generation and the
same pinned search returning the record.

## 7. `okg mcp-serve` refuses `mcp_role_cannot_login`

`okg mcp-serve` reads the graph as the `okg_mcp` database role. Migration
and the bundled `ops/pg` first-boot script create that role without a
password, because PostgreSQL roles are shared by every database on the server.
On a local database, give it a random password, recorded in an
owner-only file:

```bash
uv run okg db role-passwords --apply --deployment <deployment>
```

On another PostgreSQL server, export a connection string for that role and
reconcile it:

```bash
export OKG_MCP_RO_DSN='postgresql://okg_mcp:<password>@<host>:<port>/<database>'
uv run okg runtime bootstrap --deployment <deployment>
```

The bootstrap sets the role's login and password and proves the connection. A
connection string for a role that cannot log in, or with a stale password, is
refused with `mcp_read_dsn_unusable`.

## Still stuck

Capture the failing output and file a structured report:

```bash
uv run okg deployment bug-report \
  --deployment <deployment> --repo <owner/name> \
  --summary "First-run failure after bounded diagnosis" --json
```

Review the redacted preview and follow
[Report an unrecognized deployment failure](../index.md#report-an-unrecognized-deployment-failure)
to create the issue from it.
