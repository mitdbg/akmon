---
audience: user, data owner, developer
type: tutorial
canonical_id: quickstart
section: get-started
visibility: primary
status: active
owner: documentation
---
# Quickstart

This page gets your agent a cited answer from your own data. It takes about
15 minutes after the database image downloads (about 6.5 GB the first time).
You build a graph from one of your Git repositories, add a folder of your own
documents, connect your agent, and ask it a question. The answer names the
file it came from and the version of the graph it read.

Three terms appear throughout:

- A **deployment** is one graph: its configuration, its database, and the
  sources it reads.
- A **source** is one body of data the deployment reads, such as a Git
  repository or a folder of documents.
- A **published version** is a complete, checked snapshot of the graph.
  Searches always read one. Output calls it a *generation* and names it by
  its `generation_id`.

## Before you start

You need:

- **Linux or macOS.**
- [Git](https://git-scm.com/downloads).
- [`uv`](https://docs.astral.sh/uv/getting-started/installation/). It
  installs the Python AKMON needs (`>=3.12,<3.14`).
- [Docker](https://docs.docker.com/engine/install/) with Compose v2
  (`docker compose`), running.
- **About 10 GB of free disk.** The database image is about 6.5 GB, the
  Python environment about 0.7 GB, and the package cache about 1.5 GB. A
  small repository's graph takes a few hundred MB.
- **A free port on `127.0.0.1`** for the bundled database: 5433, or another
  you choose in step 1.
- A Git repository of your own to read, and a folder of Markdown, text, JSON
  or YAML files.
- An agent that supports MCP (the Model Context Protocol, the standard way
  agents call tools) for steps 4 and 5, such as Claude Code, Codex, Claude
  Desktop or Cursor.

**Bringing your own PostgreSQL instead?** It must be PostgreSQL 17 with
these extensions available: `timescaledb` (2.15 or later), `vector` (0.7 or
later), `pg_textsearch` (1.0 or later), `pg_trgm`, `fuzzystrmatch` and
`btree_gist`. `timescaledb` and `pg_textsearch` must be listed in
`shared_preload_libraries`. The database user you give AKMON must be able to
create roles and set their passwords. Skip the database part of step 1 and give your
database's address wherever this page writes the bundled one.
[Keep the password off the command line](../operate/install.md#keep-the-password-off-the-command-line)
shows how to give it without typing the password into a command.

**What leaves your machine.** Setup downloads Python packages, the database
image and, on the first import, the code-parsing grammars. The graph this
page builds makes no model calls and sends none of your data anywhere; it
reads your files and writes to the local database only. Your agent's own
model calls in steps 4 and 5 go wherever that agent sends them.

## 1. Get AKMON and start the database

```bash
git clone --branch main https://github.com/mitdbg/akmon.git akmon
cd akmon
uv sync
source .venv/bin/activate
okg --version
```

If `uv sync` times out, run `UV_HTTP_TIMEOUT=300 uv sync` until it
succeeds; until then, `okg --version` may run a different `okg` already on
your `PATH`. The last command prints the version, for example `okg 0.1.0`. The rest of
this page runs `okg` in this activated environment. In a new terminal, run
`source .venv/bin/activate` from the `akmon` folder again. Some messages
print `uv run okg ...`; in the activated environment, plain `okg ...` is the
same command.

Start the bundled database. The first build downloads a large base image.
`--wait` returns once the database passes its health check:

```bash
cd ops/pg
docker compose up -d --build --wait
```

If port 5433 is taken, choose another, for example
`OKG_PG_PORT=5434 docker compose up -d --build --wait`, and use that port
wherever this page writes 5433. If this machine already runs this database
for another graph, set these three first, in the same terminal, so the new
one gets its own Compose project (the first is usually `okg-pg`), data
volume, container and port.
Both graphs still share the image name `okg-pg17:local`: a build from a
different checkout changes the image both start from next time, so build both from the same
AKMON version. Set:

```bash
export COMPOSE_PROJECT_NAME=okg2 OKG_PG_CONTAINER=okg2-pg OKG_PG_PORT=5434
```

**Check:** the database answers.

```bash
docker compose exec db pg_isready -U postgres -d okg
```

It prints `accepting connections`. Go back to the `akmon` folder with
`cd ../..`. The database listens only on this
machine, at `127.0.0.1:5433`. Its name is `okg`, the user is `postgres` and
the password is `okg`.

## 2. Build a graph from one of your repositories

Pick a short name for the deployment, such as `my-app`. Below it is
`<name>`, and `<repo>` is the absolute path to your repository's top folder.

```bash
okg install --profile workspace-graph --deployment-name <name> \
  --repo-root <repo> \
  --postgres-dsn postgresql://postgres:okg@127.0.0.1:5433/okg
```

This one command sets up the database, reads the repository's files, code,
commits and tests, and publishes the first version of the graph. A small
repository takes about a minute. It prints the folder where it wrote the
deployment's configuration (`dir:`). If you use Codex, it also reads your
Codex session logs in `~/.codex`. Its output ends with a `next:` list. Skip
`okg migrate` and `okg run --once`; the install already ran them. The agent
steps in it are step 4 below. `run_outcome` may read `no_change` next to
`first publish complete`: the import published the graph, and the closing
run found nothing new to add.

**Check:** the graph is published.

```bash
okg status --deployment <name>
```

Under `publishing`, `latest_published_status` reads `published`, and
`latest_published_node_count` and `latest_published_edge_count` are above
zero. `live_subtypes` lists what was read, such as `git_commit`,
`code_symbol` and `source_file`. The per-source fact counts under `sources`
can read 0 even after a publish; read the counts under `publishing`.

If the install stops, fix what it names and run the same command again with
`--force --yes`. When it stops at the `ingest` stage, the sources that worked
are usually already published; `okg status` shows what was published.

## 3. Add a folder of your own documents

Preview the folder first. Pick a short name for the source, such as `notes`:

```bash
okg deployment add-source <name> notes --type local-folder \
  --setting root=/absolute/path/to/folder
```

This changes nothing. It prints how many files match and a few of the
records it would add. By default it reads `.md`, `.txt`, `.json` and `.yaml`
files. For other text files, name the patterns as a JSON list, for example
`--setting 'include=["**/*.rst"]'`.

Add it:

```bash
okg deployment add-source <name> notes --type local-folder \
  --setting root=/absolute/path/to/folder --apply --yes
```

It imports the folder, checks the result, and publishes a new version. The
output starts with `Source 'notes': published`, names the new
`Published graph version`, and ends with one record you can search for.

**Check:** a search finds a word from one of the files.

```bash
okg search --deployment <name> --query '<a word from a file>' \
  --subtype document --limit 3
```

Each result is a `document`, named after its file, for example
`doc:notes:setup.md`. The first line ends with the generation the search
read. Without `--subtype`, the same search also returns passages
(`document_chunk`) and matching code and commits from step 2.

## 4. Connect your agent

`okg mcp-serve --deployment <name>` is a standard MCP server over stdio.
`okg agent connect` registers it with your agents for you.

`okg install` already tried, and printed `Connecting an agent waits for the
database roles the MCP server logs in as to get passwords`: the server logs
in to the database as its own read-only role, and a new database gives that
role no password. Give it one:

```bash
okg db role-passwords --apply --deployment <name>
```

It sets random passwords, proves each login, and records them in a file only
you can read (`role-secrets` under `~/.config/okg/`). It ends with
`ok: True`. The server reads that file itself, so no password goes into any
agent's configuration.

Then connect every agent found on this machine (Claude Code, Claude
Desktop, Cursor and Codex). If you use Claude Desktop, quit it first:
connect refuses to change its settings while it runs.

```bash
okg agent connect --deployment <name>
```

It starts the server once to check it, then adds an entry named
`okg-<name>` to each agent and installs the bundled skills. Start a new session
in each agent afterwards, and reopen Claude Desktop. For an agent
it does not cover, or to register the server by hand, see
[Connect an agent](../use/agents/index.md).

**A database on another host.** `okg db role-passwords` refuses a database
that is not on this machine. The two roles belong to the whole database
server, not to one deployment, so their passwords are left to whoever runs
that server. `okg install` already created both roles without a login. Once
per server, as an administrator, in `psql`:

```sql
ALTER ROLE okg_mcp WITH LOGIN;
ALTER ROLE app_rw WITH LOGIN;
\password okg_mcp
\password app_rw
```

`\password` asks for each password and sends the server only a hash of it.
Then put the connection strings in the deployment's `.okg/local.env`, in the
folder `okg install` printed as `dir:`, readable only by you:

```bash
cd <dir>
mkdir -p .okg && touch .okg/local.env && chmod 600 .okg/local.env
# edit .okg/local.env:
#   OKG_DSN=postgresql://<user>:<password>@<host>:5432/<dbname>
#   OKG_MCP_RO_DSN=postgresql://okg_mcp:<password>@<host>:5432/<dbname>
#   OKG_MCP_RW_DSN=postgresql://app_rw:<password>@<host>:5432/<dbname>
```

`OKG_DSN` is the database you installed with. `OKG_MCP_RO_DSN` is for
reads. The server reads `OKG_MCP_RW_DSN` only with writes on
(`okg agent connect --writes`); leave it out otherwise. Then run
`okg agent connect --deployment <name>`. The agent entry holds only the path
of this file, never a password.
Run the check below from `<dir>` as well: `okg mcp-serve` finds
`.okg/local.env` from the folder it starts in.

**Check:** the server lists its seven tools. Run the check under
[Check it](../use/agents/index.md#check-it) on Connect an agent; it needs no
agent and prints `['inspect', 'search', 'expand', 'filter', 'map',
'aggregate', 'query']`.

In a new agent session, ask which tools the `okg-<name>` server offers. It lists
the same seven.

## 5. Ask a first question

Start a new agent session and ask about something in the folder you added,
for example:

> Using the tools from the `okg-<name>` server, what do my notes say about `<a topic from the
> folder>`? Quote the passage and cite the file and the graph version.

The agent should make two kinds of tool calls:

1. `search` with your topic as the `query`. Each result names the record,
   the file it came from (`cite "notes/<file>"`: the source name, then the
   file's path inside the folder), the source (`source <name>.notes`), and
   the `generation` it read. Code and commits from step 2 can rank above
   your documents.
2. `inspect` with `target` set to
   `{"kind": "text", "node_id": "<a result's id>"}` to read
   the passage's full text. Its first line names the same generation.

**Check:** the answer quotes your file, names it (for example
`notes/setup.md`), and names the `generation_id` it read. Run
`okg status --deployment <name>`: that `generation_id` is the
`latest_published_generation`. Every later answer can be traced the same
way, to the source record and the published version it came from.

If the agent answers without calling the tools, ask it again to use the
`okg-<name>` tools.

## Next steps

- **Bring in more of your data.** [Connect your data](../use/sources/index.md)
  covers GitHub issues and pull requests, keeping a source current, and
  removing one. [Configure a deployment](../operate/configure.md) covers the
  readers no command adds yet.
- **Keep it current.** The graph changes only when an import runs. Run
  `okg ingest --deployment <name>` after your data changes, or run the
  [background worker](../operate/deployments/runtime.md).
- **Run it as a service.** [Plan a deployment](../operate/plan.md) before
  you connect private data or share it with others.
- **Reference.** [Command line](../reference/cli.md),
  [MCP graph reads](../reference/mcp.md) and the
  [glossary](../reference/glossary.md).
- **Something failed.** See
  [First-run triage](../runbooks/troubleshooting/first-run.md).
