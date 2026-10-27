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
15 minutes after the database image downloads (about 1.9 GB to download,
6.5 GB on disk).
You install a released version of AKMON, start its database, build a graph
from one of your Git repositories, add a folder of your own documents,
connect your agent, and ask it a question. The answer names the file it came
from and the version of the graph it read. The steps are the same on Docker
and on podman.

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
- [Docker](https://docs.docker.com/engine/install/) or
  [podman](https://podman.io/docs/installation), running. Compose is not
  needed.
- **About 10 GB of free disk.** The database image is about 6.5 GB on disk, the
  Python environment about 0.7 GB, and the package cache about 1.5 GB. A
  small repository's graph takes a few hundred MB.
- **A free port on `127.0.0.1`** for the bundled database: 5433, or another
  you choose in step 1.
- A Git repository of your own to read, and a folder of Markdown, text, JSON
  or YAML files, or of PDFs. Step 3 does not read web pages, Jira, Confluence
  or mail; the [capabilities reference](../reference/capabilities.md#source-families)
  lists what each route reads.
- An agent that supports MCP (the Model Context Protocol, the standard way
  agents call tools) for steps 4 and 5, such as Claude Code, Codex, Claude
  Desktop or Cursor.

**Bringing your own PostgreSQL instead?** It must be PostgreSQL 17 with
these extensions available: `timescaledb` (2.15 or later), `vector` (0.7 or
later), `pg_textsearch` (1.0 or later), `pg_trgm`, `fuzzystrmatch` and
`btree_gist`. `timescaledb` and `pg_textsearch` must be listed in
`shared_preload_libraries`. (`pg_cron` is optional.) The database user you
give AKMON must be able to create roles and set their passwords. Skip the
database part of step 1 and give your database's address wherever this page
writes the bundled one.
[Keep the password off the command line](../operate/install.md#keep-the-password-off-the-command-line)
shows how to give it without typing the password into a command.

**What leaves your machine.** Setup downloads Python packages, the database
image and, on the first import, the code-parsing grammars. The graph this
page builds makes no model calls and sends none of your data anywhere; it
reads your files and writes to the local database only. Your agent's own
model calls in steps 4 and 5 go wherever that agent sends them.

## 1. Get AKMON and start the database

Choose the release to install. The commands below work for any release; use
the newest number on the
[releases page](https://github.com/mitdbg/akmon/releases).

```bash
export AKMON_VERSION=0.4.0
git clone --depth 1 --branch "v${AKMON_VERSION}" https://github.com/mitdbg/akmon.git akmon
cd akmon
uv sync --locked
source .venv/bin/activate
okg --version
```

The last command prints the version you chose, for example `okg 0.4.0`. If
`uv sync` times out, run `UV_HTTP_TIMEOUT=300 uv sync --locked` until it
succeeds; until then, `okg --version` may run a different `okg` already on
your `PATH`. The rest of this page runs `okg` in this activated environment.
In a new terminal, run `source .venv/bin/activate` from the `akmon` folder
again. Some messages print `uv run okg ...`; in the activated environment,
plain `okg ...` is the same command. To install the release's wheel instead
of a checkout, see [Install from a wheel](../operate/install-packaged.md).

Start the database from the image the release publishes. It holds PostgreSQL
17 with the extensions AKMON needs. On podman, type `podman` where this page
writes `docker`; the commands and flags are the same.

```bash
docker run -d --name okg-pg --init --no-healthcheck --shm-size 2g \
  -p 127.0.0.1:5433:5432 \
  -e POSTGRES_PASSWORD=okg -e POSTGRES_DB=okg \
  -e "POSTGRES_INITDB_ARGS=--locale=C --encoding=UTF8" \
  -v okg_pgdata:/home/"postgres"/pgdata/data \
  "ghcr.io/mitdbg/akmon-postgres:v${AKMON_VERSION}" \
  postgres -c shared_preload_libraries=timescaledb,pg_cron,pg_textsearch \
  -c cron.database_name=okg -c max_connections=100 \
  -c max_worker_processes=32 -c wal_compression=zstd
```

`--no-healthcheck` is there for podman, whose health checks need a systemd
user session that many servers lack. The database listens only on this
machine, at `127.0.0.1:5433`. Its name is `okg`, the user is `postgres` and
the password is `okg`. If port 5433 is taken, change the number before the
first colon in `-p` and use that port wherever this page writes 5433. If this
machine already runs a database for another graph, also change the name
after `--name` and the volume name `okg_pgdata`. (The quotes in the `-v` path
do not change the path.)

**Check:** the container is running, then the database answers.

```bash
docker ps --filter name=okg-pg
```

This lists `okg-pg` with a status that starts with `Up`. If `docker run`
printed an error, fix that first. If it printed an id but the list is empty,
run `docker ps -a --filter name=^okg-pg$` and `docker logs okg-pg` to see why
it stopped. On a shared server where `podman run` stops with `crun: join
keyctl: Disk quota exceeded`, see [First-run triage](../runbooks/troubleshooting/first-run.md#0b-podman-run-stops-with-crun-join-keyctl-disk-quota-exceeded).
Then wait for the database. The first start takes a few seconds.

```bash
until docker exec okg-pg pg_isready -h 127.0.0.1 -U postgres -d okg; do sleep 2; done
```

It ends with `accepting connections`. If it never ends, run
`docker logs okg-pg` to see why the database did not start.

**Prefer to build the image yourself?** From the `akmon` folder, run
`cd ops/pg && docker compose up -d --build --wait` (Docker; podman-compose
rejects `--wait`). It downloads the same large base image. Compose reads
`OKG_PG_PORT`, `OKG_PG_CONTAINER`, `OKG_PG_PASSWORD`, `OKG_PG_IMAGE` and
`COMPOSE_PROJECT_NAME`; the top of `ops/pg/docker-compose.yaml` lists them.
`OKG_PG_IMAGE` is new in 0.3.0. Give a second graph its own project,
container, port and image name: a build from another checkout otherwise
changes the image both start from.

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
repository takes about a minute. If you use Codex, it also reads your
Codex session logs in `~/.codex`. The output is about ten lines. Look for
`Deployment folder:`, the folder where it wrote the deployment's
configuration, and for `The graph now holds N records and M links`, which
says the first version of the graph is published. It also prints one search
to try and the commands that connect an agent (step 4 below). The install
already ran the database migration and the first publish, so there is
nothing else to run yet. The install prints a warning only when you need to
act on it. Adding `--verbose` to the install command prints the full
report, and `--json` prints it as a machine-readable document.

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
may be staged but not published; `okg status` shows what was published. If
the message names `catalog_manifest_drifted` (the final publish was refused
after a `--force` over an existing database), run `okg catalog ownership claim --deployment <name>`, then
`okg run --once --deployment <name> --apply`.

## 3. Add a folder of your own documents

Preview the folder first. Pick a short name for the source, such as `notes`:

```bash
okg deployment add-source <name> notes --type local-folder \
  --setting root=/absolute/path/to/folder
```

This changes nothing. It prints how many files match and a few of the
records it would add. By default it reads `.md`, `.txt`, `.json` and `.yaml`
files. For other text files, name the patterns as a JSON list, for example
`--setting 'include=["**/*.rst"]'`. The list replaces the default, so name
every pattern you want. To read PDF text, add `--setting 'include=["**/*.pdf"]'`;
see [Local folder](../use/sources/local-folder.md).

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
`doc:notes:setup.md`, with the matching passage. The source it came from
(`source <name>.notes`) is named once in the header, not on each row. Without
`--subtype`, the same search also returns passages (`document_chunk`) and
matching code and commits from step 2. The header names one source only when
all rows come from that source.

Every `okg search` in text form prints the line `! search profile: none
(unscoped)`. It says no search profile was applied. It is not a warning, and
`--subtype` and `--source-family` still narrow the results. When `--scope`
names a profile, or the deployment's `search.default_profile` applies, the line
names that profile instead. `--json` does not print it; see
[Query and trace the graph](../use/search/index.md#find-records).

## 4. Connect your agent

`okg mcp-serve --deployment <name>` is a standard MCP server over stdio.
`okg agent connect` registers it with your agents for you.

`okg install` already tried, and printed a line such as `Cursor waits for the
database roles to get passwords. In order:` (it names the agents it
found): the server logs in to the database as its own read-only role, and a
new database gives that role no password. Give it one:

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

**If Codex is not installed,** `okg agent connect` can still report Codex as
`connected` and write its configuration file, because the install step may
already have created Codex's folder. Name your own agent with
`okg agent connect --deployment <name> --agent <your agent>` (`claude-code`,
`claude-desktop`, `cursor` or `codex`). If none of the four is installed,
install one first, or follow [Connect any other
agent](../use/agents/index.md#connect-any-other-agent) to write the entry by
hand.

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
folder `okg install` printed as `Deployment folder:`, readable only by you:

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

**Check:** the server lists its eight tools. `okg agent connect` already
printed `server check: started from / and listed 8 tools`. To run the check
yourself, use the one under [Check it](../use/agents/index.md#check-it) on
Connect an agent; it needs no agent and prints `['inspect', 'search',
'expand', 'filter', 'map', 'aggregate', 'query', 'run']`. If it prints `[]` on a busy
machine, run it again.

In a new agent session, ask which tools the `okg-<name>` server offers. It lists
the same eight.

## 5. Ask a first question

Start a new agent session and ask about something in the folder you added,
for example:

> Using the tools from the `okg-<name>` server, what do my notes say about `<a topic from the
> folder>`? Quote the passage, name the file, and give the graph version
> (`inspect` the graph for it).

The agent should make two kinds of tool calls:

1. `search` with your topic as the `query`. Each result names the record
   (`doc:notes:<file>`), the file it came from, the source
   (`source <name>.notes`), and the matching passage. Code and commits from
   step 2 can rank above your documents.
2. `inspect` with `target` set to
   `{"kind": "text", "node_id": "<a result's id>"}` to read
   the passage's full text.

Read replies do not name the graph version. To get it, the agent calls
`inspect` with `target` set to `{"kind": "graph"}`: its first line ends with
`generation <id>`.

**Check:** the answer quotes your file, names it (for example
`concepts.rst`), and names the `generation` it got from `inspect`. Run
`okg status --deployment <name>`: that id is the `latest_published_generation`.
Every later answer can be traced the same way, to the source record and the
published version it came from.

If the agent answers without calling the tools, ask it again to use the
`okg-<name>` tools.

## Next steps

- **Bring in more of your data.** [Connect your data](../use/sources/index.md)
  covers GitHub issues and pull requests, keeping a source current, and
  removing one. Web pages, Jira, Confluence and mail have no one-command
  source yet. [Configure a deployment](../operate/configure.md)
  covers the readers no command adds yet.
- **Stop or remove the database.** `docker stop okg-pg` stops it and keeps
  the graph; `docker start okg-pg` brings it back. To delete the graph and its
  data, run `docker rm -f okg-pg` and then `docker volume rm okg_pgdata`.
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
