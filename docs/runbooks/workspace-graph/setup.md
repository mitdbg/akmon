---
audience: developer, operator
type: how-to
canonical_id: runbook-workspace-graph-quickstart
---
# Workspace Graph Quickstart


A Workspace Graph maps one git-backed software workspace per deployment. It
connects code, docs, tests, change records, git history, agent sessions, and
verification output so developers and coding agents can find context before
editing.

For a new project, create a deployment from the reusable `workspace-graph`
profile pointed at the git repository you want to index.

The reusable profile can point at another repository root, but not every
arbitrary folder or optional evidence source will produce data without
project-specific configuration. For multiple projects,
create one deployment per git-backed workspace.

Use it to answer questions like:

- Which files and tests are related to this change?
- What prior work explains this part of the system?
- Is the graph fresh enough to trust right now?
- Why did the latest graph publish fail?

Setup creates a deployment-owned Postgres database, writes deployment config,
ingests the configured sources, and publishes a stable snapshot called a
generation. It does not index your whole machine.

## Before You Start

Run `uv run okg ...` from the OKG framework checkout. A global `okg` entrypoint
can lag behind the checkout.

You need:

- Python 3.12 through the OKG framework `uv` environment.
- Docker Compose, or another local Postgres instance you control.
- Postgres client tools on your shell path: `createdb` and `psql`.
- The repo's `code` and `quality` extras when building the full graph.
- A usable tree-sitter grammar cache: every code-structure source loads all
  grammars when it starts, downloading them on first use into the per-user
  cache. When the home directory is not writable (an unattended job on an
  AFS home, for example), export
  `TREE_SITTER_LANGUAGE_PACK_CACHE_DIR=<dir>` first, where `<dir>` is owned
  and writable by that user and not group- or other-writable.
- A git repository at `<repo-root>`.

Choose these values before you start:

| Placeholder | Example | Meaning |
|---|---|---|
| `<deployment>` | `my-app-workspace` | The graph identity and deployment name. |
| `<repo-root>` | `/Users/you/projects/my-app` | The repository or workspace to index. |
| `<db-name>` | `okg_my_app_workspace` | The Postgres database this graph owns. |
| `<mcp-port>` | `18771` | A local port reserved for this graph's MCP/visualization companion. |
| `<deployment-dir>` | `/path/to/deployments/my-app-workspace` | The deployment directory printed by `okg install`; with `--deployments-root`, this is that root plus `<deployment>`. |

## 1. Start Postgres

The bundled local stack listens on port `5433` and uses password `okg`:

```bash
cd ops/pg
docker compose up -d --build
cd ../..
```

Create the database if it does not exist:

```bash
PGPASSWORD=okg createdb -h 127.0.0.1 -p 5433 -U postgres <db-name>
```

If it already exists, check that it responds:

```bash
PGPASSWORD=okg psql -h 127.0.0.1 -p 5433 -U postgres \
  -d <db-name> \
  -c 'select 1'
```

## 2. Set Up The Deployment

Start setup with `okg install` and the reusable Workspace Graph profile. It
writes the deployment files for the repository you pass as `<repo-root>`.
Commit those files before the first publish so each generation can record the
exact deployment revision and tree:

```bash
uv run --extra code --extra quality okg install \
  --profile workspace-graph \
  --deployment-name <deployment> \
  --repo-root <repo-root> \
  --postgres-dsn "postgresql://postgres:okg@127.0.0.1:5433/<db-name>" \
  --mcp-port <mcp-port> \
  --no-publish \
  --json
```

`<repo-root>` must be the top-level directory of a git worktree. For deployment
files under a specific directory instead of user state, add
`--deployments-root /path/to/deployments`; for later commands, export the same
root as `OKG_DEPLOYMENTS_DIR`.

Initialize and commit the generated deployment directory, or add it to an
existing deployment repository and commit it there:

```bash
DEPLOYMENT_DIR=<deployment-dir>
git -C "$DEPLOYMENT_DIR" init
git -C "$DEPLOYMENT_DIR" add -A
git -C "$DEPLOYMENT_DIR" commit -m "Add <deployment> deployment"
```

Publishing refuses an uncommitted deployment or a deployment outside Git
because neither has an immutable deployment revision and tree to record.

Older scripts that call the `init` verb keep working: it is now an alias
for this command, and says so when you run it.

Inspect the deployment identity and database authority:

```bash
uv run okg deployment env <deployment> --json
```

The report should show your deployment name, your database, and identity status
`ok`.

## 3. Verify The Graph

After committing the deployment, provision the graph. This supported bootstrap
installs the required PostgreSQL extensions before it migrates the schema,
applies the catalog, ingests the configured sources, and publishes:

```bash
uv run --extra code --extra quality okg provision \
  --deployment <deployment> \
  --publish-once \
  --json
```

Then check that setup published a generation:

```bash
uv run okg --json status --deployment <deployment>
```

The report should show:

- `latest_published_status` is `published`
- `latest_published_node_count` and `latest_published_edge_count` are non-zero
- `pending_generation` is `null`

For more detail, copy `latest_published_generation` from the status report:

```bash
uv run okg generation describe <generation-id> \
  --deployment <deployment> \
  --json
```

`okg dogfood status` judges a deployment by the profile in
`readiness.profile` (`dogfood`, `ci` or `standard`). This profile writes
`standard`; a deployment that declares nothing is judged as `standard` too.

- `standard` keeps every real outage red: an unreachable database, no
  published generation, a failed MCP read, or a source registry that does not
  load. A source marked `required_for_baseline` that is missing or older than
  14 days makes the graph `stale_context`. Code drift is a warning, and the
  runtime checks apply only when `runtime.enabled` is true.
- If no source is marked `required_for_baseline`, nothing is required to be
  fresh: `source_freshness` reads `not_checked` with reason
  `no_baseline_sources_declared`, and a `sources.freshness_advisory` warning
  lists declared sources older than 14 days. The chat banner and the console
  show no staleness in that case. Mark the sources you rely on
  `required_for_baseline` to make staleness visible.

For a team workspace that runs the supervised worker, declare
`readiness.profile: dogfood` and run the dogfood readiness checks:

```bash
uv run okg dogfood status --deployment <deployment> --json
uv run okg deployment ready <deployment> --checks dogfood --json
```

Dogfood readiness is stricter than the one-shot quickstart. It can report
runtime-worker, queue, or source-freshness blockers until you start continuous
sync. If it reports a blocker, treat the graph as background context only and
use live files, git, tests, and command output as the implementation truth
until readiness is clean.

## 4. Try A Search

Once the graph is ready, search it from the CLI:

```bash
export OKG_NOMOS_SUBJECT=subject:human_operator

uv run okg search \
  --deployment <deployment> \
  --query "publisher generation blocked" \
  --method lexical \
  --limit 10
```

`OKG_NOMOS_SUBJECT` identifies the local caller to the graph's read policy.
The export is optional for a local operator: with no subject env set, the
CLI read path presents the manifest's operator read principal
(`subject:human_operator` when declared), which the seeded baseline grants.
For automated agents, use `subject:agent` instead.

Agents should prefer the MCP read tools when policy allows them. Start with
`inspect`, pin the returned generation id, then use `search`, `expand`,
`filter`, `map`, `aggregate`, or bounded `query` over `okg.v_*` views.

## Store Function Text (Optional)

A new graph holds each function's signature and docstring, not its body, so
"what does this function do" has no answer in it. The `code_text` source
stores the bodies. It is **off by default** because it is the largest thing
you can add: about 20 times the code's size in the database.

Measured on this project's own source repository (20,575 tracked files),
plus or minus 50%, before any history growth:

| Scope | Files | Stored text | Database |
|---|---|---|---|
| Everything the source matches, tests included | 5,058 | 104 MB | about 2.1 GB |
| Default (tests left out) | 2,270 | 56 MB | about 1.15 GB |
| `src/` only | 1,638 | 47 MB | about 0.95 GB |

**Never turn it on first in a live graph, one people read today.** Trial it
on a copy or a disposable deployment, check the database size and the publish
time, then decide.

To turn it on:

1. In the deployment's `source_registry.yaml`, set `enabled_by_default: true`
   under `code_text`. A deployment installed from the profile after this
   source existed already has that entry and an `extractors.yaml` beside the
   registry. An older deployment has neither, because the installer writes
   profile files only once, at install. For one of those, first:
   - copy `extractors.yaml` from the okg package folder
     `okg/substrate/library/profiles/workspace-graph/` into the deployment
     folder, next to `source_registry.yaml`;
   - copy the `code_text:` block from `source-defaults/code_text.yaml` in that
     same folder under `sources:` in `source_registry.yaml`, replace
     `${deployment_name}` with the deployment's name and `${repo_root}` with
     the repository path (leave `${deployment_dir}` as written), and set
     `enabled_by_default: true`;
   - add `doc_snapshot` under `modules:` in `deployment.yaml`, then run
     `okg catalog load --deployment <deployment> --apply`; without it the
     publish is blocked.
2. Commit that edit. An uncommitted change to a deployment kept in git blocks
   every publish.
3. Run `uv run okg catalog ownership claim --deployment <deployment>`. A
   registry edit changes what the deployment publishes, so the next publish
   is refused until you claim the new manifest.
4. Run `uv run okg ingest --deployment <deployment> --progress`, then the
   `okg run --once` command from [Refresh Later](#refresh-later).

`okg ingest --deployment <deployment> --include code_text` reads it once, but
the worker then leaves it stale; do not use that to keep it current.

- **Size cap.** A corpus over 64 MiB (`max_total_bytes`) is refused whole,
  with the total and the five largest folders; nothing is stored. Add those
  folders to `exclude_globs`, or raise the cap once you have priced it above.
  A file over 1,000,000 bytes is recorded without text.
- **Events can pass the cap.** The cap is checked when a full read runs. A
  change event for a few files is not checked, so the corpus can grow past
  the cap between full reads; the next full read refuses it. A full read that
  finds no git change is not checked either, so lowering the cap takes effect
  at the next change.
- **Tests are left out** (`tests/**` and `**/tests/**`). To store them, delete
  those two lines from `exclude_globs`; the whole corpus then needs a larger
  `max_total_bytes`.
- **Not a secret scanner.** The source skips common credential file names
  (`.env`, `*.pem`, `secrets/**`, and similar), vendored and generated code. It
  reads everything else, and a secret in a code file becomes searchable. For a
  repository whose graph readers are not its committers, narrow `include_globs`
  or leave the source off.
- **Reading it.** Search for an exact name with `method: grep`. `inspect` with
  `kind: text` on a function's `symbol:` node names the document that holds it.

Every edit near the top of a file re-stores the later chunks of that file;
that growth is not measured, so trial the source before relying on it.

## Refresh Later

For day-to-day work, refresh the common repository sources:

```bash
uv run --extra code --extra quality okg ingest \
  --deployment <deployment> \
  --progress

uv run --extra code --extra quality okg run --once \
  --deployment <deployment> \
  --apply
```

For continuous local dogfood, start the runtime worker:

```bash
uv run --extra code --extra quality okg runtime bootstrap \
  --deployment <deployment> \
  --json

uv run --extra code --extra quality okg runtime worker \
  --deployment <deployment>
```

## If Something Fails

For first-run failures — manifest resolve errors, a missing database,
all sources denied, an empty publish, missing extras, denied search —
follow [First-Run Triage](/runbooks/troubleshooting/first-run/); it maps each failure
signature to its cause and fix command. Quick reference:

| Symptom | First check |
|---|---|
| `deployment env` points at the wrong database | Re-run `deployment env --write-local --dsn ...` and inspect again. |
| latest generation is `blocked` | Run `uv run okg generation describe <generation-id> --deployment <deployment> --json`. |
| sources are stale | Re-run the scoped refresh above, then check `dogfood status` again. |
| queue or DLQ blockers appear | Start from `dogfood status`; it reports the failing queue and next action. |
| graph reads are denied | Use live files and commands, then report the read-policy blocker. |

## Related Pages

- [Deployment runtime](/operate/deployments/runtime/) for worker and service
  supervision.
