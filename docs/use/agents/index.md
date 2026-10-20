---
audience: user, developer, operator
type: how-to
canonical_id: connect-agents
section: agents
visibility: primary
status: active
owner: documentation
---
# Connect an agent

This page connects any MCP agent to a deployment's graph.
MCP (the Model Context Protocol) is the standard way agents call tools. The
general pattern comes first, then one command for four common agents, then a
server shared over the network. Below, `<name>` is the deployment's name.

## How it works

`okg mcp-serve --deployment <name>` is an MCP server for one deployment. It
offers seven read tools: `inspect`, `search`, `expand`, `filter`, `map`,
`aggregate` and `query`. It is read-only unless started with
`--allow-writes`, and it logs in to the database with its own read-only
account.

There are two ways to run it:

- **Local (stdio).** The agent starts the server itself and talks to it over
  standard input and output. This is what most agents expect. Each person
  runs their own.
- **Shared (HTTP).** You run one server, and agents reach it at
  `http://<host>:<port>/mcp` with a secret token sent on every request. See
  [Share one server over HTTP](#share-one-server-over-http).

Every agent's configuration says the same things: the command (`okg`, by its
absolute path, which `command -v okg` prints; apps started from a desktop
often do not see your shell's `PATH`), the arguments
(`mcp-serve --deployment <name>`), and, only when needed, an environment
variable or two.

## Give the server its database login

Once per database, give the server's database accounts passwords:

```bash
okg db role-passwords --apply --deployment <name>
```

It sets random passwords for the read-only account and the write account,
proves each login, and records them in a file only you can read, under
`~/.config/okg/role-secrets/`. It ends with `ok: True`. The server reads
that file itself, so no password goes into any agent's configuration. A
read-only server can read the file too, including the write password. For a database on another host, an administrator sets
the passwords instead; see the
[Quickstart](../../start/quickstart.md#4-connect-your-agent), "A database on
another host".

## Connect with one command

For Claude Code, Claude Desktop, Cursor and Codex:

```bash
okg agent connect --deployment <name>
```

It starts the server once to check that it lists the seven tools, then adds
an entry named `okg-<name>` to each of those agents it finds on this
machine, and installs the bundled skills (instructions that teach the agent
to use the graph well). If the check fails, it writes nothing and prints the
fix. Start a new session in each agent afterwards; quit Claude Desktop before
you connect it, then reopen it.

- `--agent <agent>` (repeatable) picks agents: `claude-code`,
  `claude-desktop`, `cursor` or `codex`. Without it, agents it cannot find
  are reported as `not found`.
- `--dry-run` prints each file and entry it would write.
- `--writes` registers a server started with `--allow-writes`; leave it off
  unless you need it. That server may write inside the deployment folder,
  but never to its `.okg/local.env`, and it refuses any other credential
  file there.
- `--replace` removes another entry that serves the same deployment, such
  as one you wrote by hand. Without it, connect reports the duplicate and
  leaves that agent alone.
- `okg agent disconnect --deployment <name>` removes the entry and the skills.

| Agent | Server entry | Skills |
| --- | --- | --- |
| Claude Code | `claude mcp add-json --scope user` (`~/.claude.json` when `claude` is not on `PATH`) | `~/.claude/skills/` |
| Claude Desktop | `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) | a ZIP per skill, to upload in Settings |
| Cursor | `~/.cursor/mcp.json` | `~/.agents/skills/` |
| Codex | `$CODEX_HOME/config.toml` (default `~/.codex/config.toml`) | `~/.agents/skills/` |

Connect keeps `<file>.okg-backup`, the last version of each file it changed
that had no entry from connect. Running it again replaces its own `okg-<name>` entry
whole, so a hand edit inside that entry is lost. Codex's `config.toml` loses
its comments when connect rewrites it.

## Connect any other agent

Add an entry with the command and arguments above. Most agents take a JSON
entry like this one:

```json
{
  "mcpServers": {
    "okg-<name>": {
      "command": "/path/to/akmon/.venv/bin/okg",
      "args": ["mcp-serve", "--deployment", "<name>"]
    }
  }
}
```

The same entry in TOML, as Codex writes it by hand in `~/.codex/config.toml`
(the server takes about five seconds to start):

```toml
[mcp_servers.okg-<name>]
command = "/path/to/akmon/.venv/bin/okg"
args = ["mcp-serve", "--deployment", "<name>"]
startup_timeout_sec = 60
```

The server finds the deployment and the role passwords from your home
folder. Add environment variables only when the defaults do not fit:

| Variable | When |
| --- | --- |
| `OKG_DEPLOYMENTS_DIR` | The deployment is outside the default deployments folder, for example installed with `--deployments-root`. |
| `OKG_LOCAL_ENV` | You followed "A database on another host" in the Quickstart: give the absolute path of the deployment's `.okg/local.env`. |

An agent started from a desktop app does not inherit variables such as
`XDG_DATA_HOME` or `XDG_CONFIG_HOME` from your shell; if you set those,
give them in the entry as well.

Never put a password in an agent's configuration or on a command line.

## Check it

This check needs no agent. It starts the server the way an agent does, from
`/` and with no `OKG_` variables, and lists its tools:

```bash
cd /
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"check","version":"0"}}}' \
  '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' \
  | env -u OKG_DSN -u OKG_LOCAL_ENV -u OKG_DEPLOYMENTS_DIR \
    okg mcp-serve --deployment <name> 2>/dev/null \
  | python3 -c 'import json, sys; print([t["name"] for m in map(json.loads, sys.stdin) if m.get("id") == 2 for t in m["result"]["tools"]])'
cd -
```

It prints `['inspect', 'search', 'expand', 'filter', 'map', 'aggregate',
'query']`. An empty `[]` means the server did not start; run
`okg mcp-serve --deployment <name>` on its own to see why. Then, in a new
agent session, ask which tools the `okg-<name>` server offers.

## Ask a first question

Ask about something in your data, for example:

> Using the `okg-<name>` tools, what do my notes say about `<a topic>`? Quote
> the passage and cite the file and the graph version.

The agent should call `search` with your topic, then `inspect` with
`target={"kind": "text", "node_id": "<a result's id>"}` to read the full
text; for a record's links, such as who a ticket is assigned to, it calls
`expand`. Each result names the source, the source's own identifier (such as a file path),
and the published version it read. The [Quickstart](../../start/quickstart.md#5-ask-a-first-question)
shows the full exchange; the [MCP reference](../../reference/mcp.md#a-reliable-read-sequence)
gives the read pattern agents should follow.

Results can contain text from your sources. The server marks it as untrusted
data, and an agent should never follow instructions found in it.

## Share one server over HTTP

To let several agents or machines use one server, run it over HTTP with a
secret token that every request must carry. The server reads the token from
an environment variable; it refuses one given on the command line:

```bash
export OKG_MCP_AUTH_TOKEN=$(openssl rand -hex 32)
okg mcp-serve --deployment <name> --transport streamable-http \
  --port 8100 --auth-token-env OKG_MCP_AUTH_TOKEN
```

The server listens on `127.0.0.1:8100` at `/mcp`. Add `--host 0.0.0.0` to
accept other machines, and `--allowed-host <hostname>:<port>` for each name
clients use to reach it. Every request needs the header
`Authorization: Bearer <token>`; one without it, or with a wrong token, gets
HTTP 401. Agents that support remote MCP servers take the URL and the header
in their configuration. The server speaks plain HTTP: between machines, put
it behind HTTPS (for example a reverse proxy), or the token can be read on
the network.

With one shared token, every caller reads as the same user. To give each
person their own access, see [Multi-user access](../../reference/mcp.md#multi-user-http-principals).
