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

For continuously refreshed local graphs, also run the dogfood readiness checks:

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
