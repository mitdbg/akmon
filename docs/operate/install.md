---
audience: operator, developer
type: how-to
canonical_id: task-install-upgrade-akmon
section: operations
visibility: primary
status: active
owner: documentation
---
# Install OKG

This page installs OKG from source and builds a graph of one Git repository:
its files, code, history and tests. At the end you can search the graph from
the command line and connect a coding agent to it.

The graph also reads two things outside the repository: your Codex session
logs in `~/.codex`, and a description of this machine's software environment.
To read Codex logs from another folder, add `--codex-sessions-root <dir>` and
`--codex-archived-sessions-root <dir>` to the install command in step 3.

## Before you start

You need:

- [Git](https://git-scm.com/downloads).
- [`uv`](https://docs.astral.sh/uv/getting-started/installation/). It
  installs the right Python for you. OKG requires Python `>=3.12,<3.14`.
- [Docker with Compose](https://docs.docker.com/engine/install/), running.
- A Git repository to index. Below, `<repo>` is the absolute path to its top
  directory.
- Later, to upgrade, restore or copy a deployment (`okg up`,
  `okg deployment backup restore`, `okg db clone`): the PostgreSQL client
  tools `pg_dump` and `pg_restore` on your `PATH`, at the database server's
  major version or newer (17 for the bundled database). Restoring needs only
  `pg_restore`. To install them:
  - macOS: `brew install libpq`, then
    `export PATH="$(brew --prefix libpq)/bin:$PATH"`.
  - Debian or Ubuntu: `postgresql-client-17` from apt.postgresql.org.
  - Elsewhere: your system's PostgreSQL 17 client package.

  When there is an upgrade to make, `okg up --plan` checks for them before
  anything changes.

Pick a short name for the graph, such as `my-app`. Below, it is `<name>`.

## 1. Get OKG

```bash
git clone --branch main https://github.com/mitdbg/akmon.git akmon
cd akmon
uv sync
source .venv/bin/activate
```

This prints the installed version:

```bash
okg --version
```

The rest of this page runs `okg` inside this activated environment. In a new
terminal, run `source .venv/bin/activate` from the `akmon` directory again, or
type `uv run okg` instead of `okg`.

## 2. Start PostgreSQL

OKG stores the graph in PostgreSQL. The repository includes a local database
with everything OKG needs:

```bash
cd ops/pg
docker compose up -d --build
cd ../..
```

It listens on `127.0.0.1:5433`, only on this machine. The database is `okg`,
the user is `postgres` and the password is `okg`. The first build downloads a
large base image.

## 3. Build the graph

```bash
okg install --profile workspace-graph --deployment-name <name> \
  --repo-root <repo> \
  --postgres-dsn postgresql://postgres:okg@127.0.0.1:5433/okg --json
```

This one command sets up the database, reads the repository, and publishes
the first version of the graph. When it succeeds it exits 0 and reports
`published`. The list of next steps it prints repeats two it has already
done, `okg migrate` and `okg run --once`; skip them.

The configuration it writes lives in
`~/.local/share/okg/deployments/<name>/`, or under `$XDG_DATA_HOME/okg/` when
that variable is set.

### Keep the password off the command line

A database with a real password should not have it typed into the command,
where it lands in your shell history and the process list. Put the
connection string in `OKG_DSN` and pass the variable's name in single
quotes:

```bash
read -rs OKG_DSN && export OKG_DSN
okg install --profile workspace-graph --deployment-name <name> \
  --repo-root <repo> --postgres-dsn '${OKG_DSN}'
```

`read -rs` waits for you to type
`postgresql://<user>:<password>@<host>:5432/<dbname>` without echoing it or saving
it in history. The single quotes stop your shell from expanding
`${OKG_DSN}`: `okg install` expands it itself, and every profile accepts
this form. The deployment's `deployment.yaml` records `${OKG_DSN}`, not the
password, so keep `OKG_DSN` exported in any shell that runs `okg` for this
deployment.

An agent starts `okg mcp-serve` without your shell's environment, so it never
sees an exported `OKG_DSN`. `okg agent connect` then stops and writes no
agent configuration; `okg install` still finishes, and its connect step
reports the same error. The message names the deployment's
`.okg/local.env` and prints a command (`mkdir -p ... && chmod 600 ... &&
printf ... >> .../.okg/local.env`) that copies `OKG_DSN` into that file,
readable only by you. Run it in the shell where `OKG_DSN` is exported, then
run `okg agent connect --deployment <name>` again. If `OKG_DSN` is not set
in that shell, the message asks you to add the line
`OKG_DSN=<database URL>` to the file yourself, with mode 600.

On a database on another host, `okg db role-passwords` refuses to set the
passwords of the roles `okg mcp-serve` logs in as, because those roles
belong to the whole server. Set them up by hand and pass `OKG_MCP_RO_DSN` and
`OKG_MCP_RW_DSN` instead; see
[Connect your agent](../start/quickstart.md#4-connect-your-agent), "A
database on another host".

Each graph needs its own database. For a second graph, create one and name it
in `--postgres-dsn`:

```bash
docker exec okg-pg psql -U postgres -c 'CREATE DATABASE okg_<name>'
```

**If you use Codex**, a session that Codex is writing during the install is
read up to that moment; the next import reads the rest. If Codex starts a new
session file while the install is reading, the install can still exit 1 at
the `ingest` stage. Everything else was read. Publish it:

```bash
okg run --once --deployment <name> --apply
```

That command may print `No change` even though it published.
Check with `okg status` in step 4.

**If the install stops for another reason**, fix what it names. It has
already written the configuration, so run the same command again with
`--force --yes` to overwrite it.

## 4. Check it and search it

```bash
okg status --deployment <name>
```

`latest_published_status` should be `published`, with non-zero node and edge
counts.

```bash
okg search --deployment <name> --query '<a function name>'
```

Search matches a file's full path from the repository root, a file name
without its extension, code names, docstrings and commit messages. It does not
match a file name with its extension unless you give the full path, and it
does not match the body text of Markdown files.

`okg doctor --deployment <name>` reports `ok` with three warnings on a new
install. `okg jobs status` exits 1 and reports `unavailable`: that only says
no background worker is running, which this page does not set up.

## Keep it up to date

The graph is a published snapshot. It does not follow the repository by
itself. To pick up new commits, run:

```bash
okg ingest --deployment <name>
```

It publishes a new version when every source succeeds.

To refresh continuously instead, see
[Deployment runtime](deployments/runtime.md).

## Next steps

- **Connect a coding agent.** `okg mcp-serve --deployment <name>` serves the
  graph's read tools over MCP, the protocol coding agents use to call tools.
  [Connect an agent](../use/agents/index.md) connects any MCP agent.
- **Add a chat site.** See [Set up chat](chat.md).
- **Add another source.** To add a folder of documents or a GitHub
  repository's issues and pull requests, see
  [Connect your data](../use/sources/index.md).
- **Configure the deployment.** What each configuration file holds, what you
  can change, and what to run after a change: see
  [Configure a deployment](configure.md).
- **Start from an empty graph instead.** Without `--profile`, step 3's
  command builds the `minimal` starter: no sources, an empty first version,
  and the record types you add with each source. Give it
  `--deployment-name <name>` and `--postgres-dsn`; see
  [Profiles](../use/profiles/index.md).
- **Index something other than a repository.** Other profiles, such as
  `academic-researcher` for a research group's papers, are listed in
  [Profiles](../use/profiles/index.md). `okg install --profile <profile>
  --deployment-name <name> --dry-run --json` names the answers a profile needs
  before it writes anything.
- **Install from a wheel or a producer-built release.** See
  [Install from a wheel or a release directory](install-packaged.md).
- **Back up or upgrade.** See [Lifecycle](lifecycle.md).
- **Something failed.** See
  [First-run triage](../runbooks/troubleshooting/first-run.md).
