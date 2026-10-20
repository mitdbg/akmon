# Install academic-researcher (one OKG per lab)

This profile scaffolds a *deployment* — one Postgres + one MCP server + every detected agent runtime's MCP config. Data sources are populated only after you approve the setup wizard. You can run multiple deployments on the same machine; each is independent.

## Prereqs (one-time)

Common (macOS and Linux; Windows TBD):

- Read access to the repository you install from. A public repository needs nothing more (see "Install without a clone" below). For a private one, a GitHub account that can read it, with working auth — any one of:
  - existing GitHub SSH keys: `export OKG_REPO_URL=git@github.com:<owner>/<repo>.git`
  - `gh auth login` (device-code flow works on headless boxes)
  - a PAT with `repo` scope via `OKG_REPO_URL=https://<USER>:<PAT>@github.com/<owner>/<repo>.git`
  - Test access: `git ls-remote <repository URL>` (or the ssh URL)
- ~10 GB free disk: the Postgres image is ~2.6 GB, plus volumes and your data.
- An agent runtime installed for the post-install setup conversation, opened
  once so its config file exists: Claude Code / Claude Desktop / codex /
  Cursor. On Linux use Claude Code
  (`curl -fsSL https://claude.ai/install.sh | bash`) or codex — Claude
  Desktop doesn't ship for Linux.

### macOS

- Docker Desktop installed and **running**. Compose v2 is bundled with it.
- Verify: `docker info >/dev/null && docker compose version`

### Linux

Docker on Linux is three separate requirements, and each missing one has
broken a real first-install (2026-06-09):

1. **Docker Engine, installed and running**:
   `sudo apt-get install docker.io && sudo systemctl enable --now docker`
   (Fedora/RHEL: `sudo dnf install docker` or Docker's official repo.)
2. **Compose v2 plugin** — packaged separately from the engine; without it
   `docker compose` falls through to the bare CLI and the image build dies
   with `unknown shorthand flag: 'd' in -d`:
   - Ubuntu/Debian: `sudo apt-get install docker-compose-v2`
   - Fedora/RHEL: `sudo dnf install docker-compose-plugin`
3. **`docker` group membership** — without it `docker info` fails with
   "permission denied" on the socket while the daemon is up:
   `sudo usermod -aG docker $USER`, then **log out and back in**
   (or `newgrp docker`).

Verify all three before installing:

```bash
docker info >/dev/null && docker compose version && echo ready
```

`install.sh` preflights every one of these and prints the matching fix on
failure, so a missing piece costs a round-trip, not a debugging session.

## Quick start (clone + run)

```bash
git clone <repository URL> ~/okg
~/okg/install.sh --profile academic-researcher
```

Use the clone URL the repository's `README.md` gives. That's the whole install. The script handles Docker preflight, installs
`uv` if absent, sets up a venv, and runs `okg install --profile
academic-researcher`. Name the profile: without `--profile`, `okg install`
builds the empty `minimal` starter instead. Smart defaults
derive most values from your OS environment: deployment name randomly
picked from the scientist roster, `--pi-name` from `git config --global
user.name` only when it looks like a full human name, and postgres host
port from Docker.

After it finishes, open the agent named in the final handoff line (Claude Desktop / codex / Cursor — whichever you have) and say:

> *"set up my OKG"*

### Optional: pass your ORCID for cleaner publication matching

```bash
~/okg/install.sh --profile academic-researcher --orcid 0000-0002-XXXX-YYYY
```

With `--orcid`, the `dblp_by_orcid` source uses DBLP's ORCID endpoint
for exact-match publication lookup. Without it, the source can fall back
to name-based search, but only when setup has a full human name from
`--pi-name` or a full-name `git config user.name`. One-token names such
as `Riley` are treated as too ambiguous and the setup agent will ask for
better identity data or skip public identity sources.

## Install without a clone

When the repository is publicly readable, the install collapses to one curl-piped command:

```bash
curl -fsSL <raw URL of install.sh on the default branch> | bash -s -- --profile academic-researcher
```

`install.sh` already defaults to HTTPS. `OKG_REPO_URL`, `OKG_REPO_BRANCH`, and `OKG_INSTALL_ROOT` remain explicit overrides for local mirrors, release branches, or nonstandard checkout locations.

## Manual fallback: HTTPS with a Personal Access Token

If your Git credential helper is unavailable but you have a GitHub PAT with `repo:read` scope:

```bash
export OKG_REPO_URL=https://<your-user>:<your-pat>@github.com/<owner>/<repo>.git
git clone "$OKG_REPO_URL" ~/okg
~/okg/install.sh --profile academic-researcher
```

Don't commit the PAT anywhere.

## What gets installed

`okg install --profile academic-researcher` boots Postgres (via Docker on a Docker-assigned host port), scaffolds your deployment, registers MCP entries with every detected agent runtime (Claude Desktop, codex CLI, Cursor), and tells you which agent to open + what to say. It does **not** ingest publications, repos, agent sessions, OpenSpec proposals, mail, calendar, notes, or documents.

The graph itself is stored in Chronos. Source ingestion applies graph
edits to Chronos registered relations on the deployment's mutable
branch, and each successful publish creates a Chronos checkpoint. That
checkpoint id is the OKG `generation_id` used by MCP, search, SQL,
and viz reads. Catalog
versions, source progress, DBOS workflow/runtime state, and audit metadata
remain in regular OKG SQL tables.

When you say "set up my OKG" in your agent, it drives source approval — publications, grants, courses, Gmail, Calendar, Slack, notes, lab page, papers, and explicit project repos — conversationally, calling the `discover_sources` / `propose_setup` / `commit_setup` MCP tools to update your registry and ingest only approved sources.

## Smart defaults

All inputs are optional. Each derived value prints its source so you can override:

- `--name` — random last name from a curated scientist/philosopher roster on first install (Einstein, Curie, Turing, Lovelace, Knuth, Plato, etc.). Memorable, identity-neutral. Pass explicitly to create or refresh a specific deployment.
- No-name reruns — if exactly one academic-researcher deployment already exists under your user-state deployments directory, `okg install` reuses it and refreshes MCP registrations. If more than one exists, pass `--name <deployment>`.
- `--pi-name` — `git config --global user.name` only when it looks like
  a full human name
- `--postgres-port` — Docker-assigned ephemeral port (race-free)
- `--orcid` — none. Pass explicitly if you want clean DBLP publication matching.

## Dry-run preview

```bash
cd ~/okg
uv run okg install --profile academic-researcher --dry-run
```

`--dry-run` prints a read-only plan: chosen MCP port, whether the Docker image needs a build, which sources require setup approval, and any agent-runtime config collision. Nothing is written, no container is started. Recommended before the real install on a new machine if you want to inspect the plan first.

After install completes, restart/open the agent named in the handoff line. Then say *"set up my OKG"* (or *"give me a lab overview"* if your sources are already wired from a previous run).

## What the install creates

A *deployment* directory at `~/.local/share/okg/deployments/<your-slug>/` (or `${XDG_DATA_HOME}/okg/deployments/<your-slug>/` when `XDG_DATA_HOME` is set) containing:

```
~/.local/share/okg/deployments/madden_pi/  # one deployment = one OKG
├── deployment.yaml                        # name + Postgres DSN + MCP port
├── source_registry.yaml                   # which sources are wired + config
├── invariants.yaml                        # health gates (PI-tuned)
└── dashboards/                            # panel SQL
```

Plus a Postgres container (`okg-<your-slug>`) on a Docker-assigned host port, an MCP server registration for every detected runtime, and entries in the detected runtime configs (`~/.codex/config.toml`, Claude Desktop, Cursor) pointing at the MCP server.

The first no-name install chooses a random scientist/philosopher last name. Override with `--name <slug>`. Pass `--no-claude-config` to skip agent-runtime registration.

Chronos serves SQL over its registered tables and owns the branchable
database details. Use `okg.v_nodes` and `okg.v_edges` through the
pinned session/query surface for OKG graph reads; use the Chronos
repository docs for Chronos branch and checkpoint internals.


## What's in your graph after install

After install, the deployment is ready but not populated with operator data. The source registry contains all profile sources, but they start disabled until the setup wizard proposes them and you approve.

The first approved setup can propose:

- **publications** — DBLP lookup via ORCID, DBLP PID, or full `pi_name`
- **NSF Award Search + DOE OSTI** — public grants from your identity hints
- **MIT courses** — when `pi_name` is present
- **Workspace Graph repos** — only when you explicitly name a repo path
- **notes, papers, CV, calendars, Gmail, Slack, lab pages** — when discovered or explicitly configured

The OKG framework checkout used for install is never indexed as your project repo unless you explicitly name that path.

**Semantic search needs embeddings enabled plus the runtime worker.** The install does NOT start one for you — enable canonical `embeddings` with an explicit verified backend in `deployment.yaml`, then run:

```bash
okg runtime worker --deployment <name>
```

The `hash` embedder needs no model download and is suitable only for deterministic smoke tests. The fixed graph column is 2560-dimensional, and this repository ships no supported production-model recipe. The runtime worker refuses a backend whose declared or observed output width is incompatible before it registers embedding work.

For convenience, `okg install --profile academic-researcher --embed-worker` starts the DBOS runtime worker at the end of the install. Off by default so you don't get silently locked into a backend before reviewing deployment config.

## Try it in Claude

```
Catch me up.
State of <advisee>.
Prep me for tomorrow with <person>.
Should I take on this PC role?
I just met with <person> — note that we agreed on X.
```

The last one writes to the graph through the `record` operator.

## Add more sources (conversational — recommended)

Open Claude Desktop, Codex, or Cursor. Say:

```
set up my OKG
```

The agent calls `discover_sources` then `propose_setup`, reads the source plan back to you, and waits for approval. On "yes" it calls `commit_setup`; the substrate updates `source_registry.yaml`, runs scoped ingest, and publishes the approved source deltas. Credentialed apps such as Gmail, Google Calendar, Slack, and GitHub are connector-backed: the plan reports the selected route, who owns auth, token custody, read-only status, and the required allowlists. If no trusted route is connected, the source is marked `needs_connection`; setup does not send you to external app setup pages.

To index code later, say:

```text
add my project repo at ~/projects/my-research-repo
```

The agent should propose repo-scoped `git_files`, `git_history`, `code_structure`, `openspec` if present, and `agent_sessions` scoped to that repo.

## Manual recovery tools

Academic-researcher setup has one supported operator path: say `set up my OKG`
to the registered agent and approve or narrow the proposed plan. Low
level CLI verbs may remain for debugging, tests, or recovery, but they
are not the product setup path and are not where Gmail, Calendar, Slack,
or GitHub connector auth is configured.

For recovery:

```bash
okg remove <source-id> --deployment <slug> # stop future emits
okg ingest --include <source-id> --deployment <slug> --inline
```

## Troubleshoot

```bash
okg doctor --deployment <your-slug>      # substrate-wide checks
okg generation list --deployment <slug>  # what's published
okg trace node <node_id>                 # full provenance for one node
okg reset --deployment <your-slug>       # remove the deployment, its Docker Postgres and data
```

## Connector-backed apps

Gmail, Google Calendar, Slack, GitHub, and similar services are handled
through connector routes:

- `host_connector`: the agent host already has the app connection and
  imports bounded normalized batches into OKG.
- `provider_mcp`: the service provider hosts or maintains the MCP
  server.
- `gateway_mcp`: a trusted local gateway, such as a messaging bridge,
  exposes already-connected channels.
- `local_mcp` / `cli_tool`: pinned local read-only routes.

First setup is read-only. It requires allowlists for labels, calendars,
channels, repos, paths, or time ranges before importing private data.
External app setup pages and hosted broker signup are not academic-PI
setup paths.

For provider evaluation rules, see
[`docs/reference/built-in-capabilities.md`](../../../../../../docs/reference/built-in-capabilities.md#connector-routes).

## Multiple labs on one machine

Each install picks free Postgres and MCP ports. Override with `--name` to disambiguate:

```bash
okg install --profile academic-researcher --orcid 0000-...-MADDEN --name madden_pi
okg install --profile academic-researcher --orcid 0000-...-KRASKA --name kraska_pi
```

Each gets its own Postgres container, MCP server, and Claude Desktop entry.
They do not share an AKMON database. Host, network, credential, and assistant
configuration still determine the full isolation boundary.

## Don't have an ORCID?

Pass `--name <slug>` and skip the ORCID-derived sources:

```bash
uv run okg install --profile academic-researcher --name madden_pi --no-claude-config
```

You'll start with an empty, ready deployment. The setup wizard can
still propose publication sources from `--pi-name`; add more precise
publication sources later by rerunning the setup planner with a narrower
source request.

## What "deployment" means

A *profile* (`academic-researcher`) is a packaged set of choices — ontology modules, source-defaults, init questions, invariants, dashboards. It lives in-tree at `src/okg/substrate/library/profiles/academic-researcher/`.

A *deployment* is an instance created from a profile. Each deployment has its own Postgres database, its own MCP server, and its own source-registry that says which sources are enabled and how each one is configured.

`okg install --profile academic-researcher` is the one-command path that creates a deployment from this profile. Under the hood it provisions Postgres (builds the substrate's `okg-pg17:local` image first run, ~5 min), scaffolds the deployment directory from this profile, registers MCP entries with detected agent runtimes, and prints a report. Source ingestion starts after the conversational setup plan is approved.

The substrate owns `okg install`; this profile contributes its own installer and flags to it through `INSTALL_EXTENSION` (see `library/profiles/academic-researcher/cli/`). Profiles without one, including the default `minimal` starter, go through the general installer.
