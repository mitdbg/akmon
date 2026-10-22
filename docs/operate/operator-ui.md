---
audience: operator, developer
type: how-to
canonical_id: operator-ui
---
# AKMON Operator UI


The Operator UI is a read-only browser tool for inspecting a running AKMON
deployment: whether its graph is healthy, what each update did, the state of
every source, and the records and relationships the graph holds.
It never changes deployment state: it issues only `GET` reads and shows loud
failures when a read returns no data.

## Start It

One command serves the UI for a deployment:

```bash
npm install && npm run operator-ui:build   # once per checkout; rebuild after UI changes
uv run --extra postgres --extra viz okg ui --deployment <name>
# → okg ui: http://127.0.0.1:8765/ (deployment=<name>, bundle=..., read-only; ...)
```

Open the printed address. `okg ui` resolves the deployment's database the
same way the other read commands do (the deployment manifest and
`.okg/local.env`; `--dsn` overrides), starts the read API, and serves the
built frontend beside it on the same port. The graph loads in the
background; until it is ready the views say so.

The frontend is TypeScript built with Vite, and the Python package does not
ship it. `okg ui` serves the bundle `npm run operator-ui:build` writes to
`.tmp/operator-ui-dist/` in the source checkout, or the directory named by
`--dist` or `$OKG_OPERATOR_UI_DIST`. With no built bundle it refuses with
`operator_ui_bundle_missing` and the build command; it never starts the read
API alone. An older bundle is served as it is; rebuild after pulling UI changes.

`okg ui` answers only `GET`, `HEAD` and `OPTIONS`. Any other request gets
HTTP `405` with `operator_ui_read_only`, before it reaches the read API.
`HEAD` works for the page and its assets; the read API's routes answer `HEAD`
with their own `405`, so probe them with `GET`. `OPTIONS` reaches the read API
only for its cross-origin preflight; no handler runs.

`okg ui` serves loopback only: `--host` must be `localhost`, an address in
`127.0.0.0/8`, or `::1`. Any other host, including `0.0.0.0`, `::` and an
IPv4-mapped address such as `::ffff:127.0.0.1`, is refused with
`operator_ui_bind_refused`, whether or not `OKG_VIZ_OPERATOR_TOKEN` is set. A
port outside 1-65535, or an `OKG_VIZ_PORT` that is not a number, is refused the
same way. To use the UI from another machine, forward the port over SSH:

```bash
ssh -L 8765:127.0.0.1:8765 <host>   # then open http://127.0.0.1:8765/ locally
```

Prerequisites:

- A deployment with at least one **published generation**. The UI reads a
  published graph; with nothing published the views load and report the
  failure. Confirm readiness first with
  `uv run okg dogfood status --deployment <name> --json` (see the operator
  [operating guide](index.md)).
- Python with the `postgres` and `viz` extras.
- **Node.js + npm**, once, to build the frontend bundle.

## How It Is Put Together

The UI has two parts, which `okg ui` runs in one process:

- a **read API** that serves `/viz/api/*` over the deployment's Postgres
  database, pinned to one
  [published generation](../reference/glossary.md#generation) per process, and
- a **single-page frontend** (Vite + React) that renders the views and reads
  `/viz/api` from the same origin.

The read API on its own (`python -m okg.viz`) deliberately does **not** serve
the app: its `/` and `/viz/` routes return an archived `410`. An "archived"
JSON blob in the browser means you opened a bare read API, not `okg ui`.

For the exhaustive per-view behavior contract (data sources, capped-data rules),
see the [Operator UI contract](../develop/ui/operator-contract.md) and the
[operator console contract](../develop/ui/operator-console.md). This page is how
to run it and what each view is for.

## What You Get

The app is the operator console. It opens on Home and has five views, one per
entry in the top bar:

| View | Route | Answers |
|---|---|---|
| Home | `#console/home` (also the bare URL) | Is this deployment healthy, and what needs attention |
| Updates | `#console/updates` | What each update attempt did, and why a blocked one stopped |
| Sources | `#console/sources` | The state, last run and freshness of every source, enricher and view |
| Explore | `#console/explore` | Search and open the records the graph holds |
| Graph | `#console/graph` | Which record types each producer made, and how they connect |

An address that is not a `#console/...` address opens Home. That includes
the bare URL, `#home` and `#health`, the two pages this app used to have, and
older views such as `#graph` and `#trace`, so an old bookmark or chat link
lands on a working page rather than an error. A `#console/<name>` address
whose name is none of the five views shows the line "No console view is
named ..." and lists the five.

## Developing The UI (two processes)

For live reload while changing the frontend, run the read API and the Vite
dev server separately.

**Terminal 1 — the read API:**

```bash
export OKG_DSN=postgres://.../okg          # or pass --dsn
export OKG_UI_TOKEN=<token>                # if the deployment is sensitive
uv run --extra postgres --extra viz \
  python -m okg.viz --deployment <name> --port 8765 \
  --auth-token-env OKG_UI_TOKEN
# → OKG viz: http://127.0.0.1:8765/viz/ (deployment=<name>, loading in background)
```

`--generation N` pins a generation. Check it
with the token on stdin:

```bash
printf 'header = "Authorization: Bearer %s"\n' "$OKG_UI_TOKEN" \
  | curl -K - -fsS http://127.0.0.1:8765/viz/api/health
```

**Terminal 2 — the frontend:**

```bash
npm install                                # once, to fetch dependencies
OKG_OPERATOR_API_BASE=http://127.0.0.1:8765 \
  npm run operator-ui:dev -- --port 5173
# open http://127.0.0.1:5173
```

`OKG_OPERATOR_API_BASE` tells the dev server where the read API is. Omit it and
the app still loads, but every `/viz/api` call returns an HTTP `200` whose JSON
body reports a failure (`status: 502`, `error: "operator UI dev
server has no OKG API target"`) — set the variable and reload.

The audit harness wraps both terminals in one launcher against the live
deployment named by `OKG_AUDIT_DEPLOYMENT`:

```bash
bash src/okg/operator_ui/web/audit/start-servers.sh   # API :8765, frontend :5173
bash src/okg/operator_ui/web/audit/stop-servers.sh    # when done
```

It sources the deployment DSN from `.okg/local.env` (override with
`OKG_AUDIT_DSN_ENV`, or export `OKG_DSN` yourself) and is idempotent — if a
server is already answering it is reused. Its read token is
`OKG_AUDIT_API_TOKEN` or the 0600 file `audit/.servers/api.token`. Outside this repository, set
`OKG_AUDIT_DEPLOYMENT=<name>`: the default is its own workspace deployment.

## Ports And Environment

`okg ui` and the bare read API (`python -m okg.viz`) share these settings;
flags override the matching env var:

| Flag | Env | Default | Purpose |
|---|---|---|---|
| `--deployment` | `OKG_DEPLOYMENT` | `okg ui`: required; read API: `default` | Deployment to pin against |
| `--dsn` | `OKG_DSN` | `okg ui`: resolved from the deployment; read API: required | Deployment Postgres DSN |
| `--generation` | — | latest published | Pin a specific generation |
| `--host` | `OKG_VIZ_HOST` | `127.0.0.1` | Bind host (`okg ui`: loopback only) |
| `--port` | `OKG_VIZ_PORT` | `8765` | Bind port (`okg ui`: 1-65535) |
| `--dist` (`okg ui` only) | `OKG_OPERATOR_UI_DIST` | `.tmp/operator-ui-dist` in the checkout | Built frontend bundle |
| `--auth-token-env` | — | unset | Variable holding the access token; see below |
| — | `OKG_VIZ_OPERATOR_TOKEN` | unset | Read API only: operator token for a non-loopback bind. `okg ui` does not use it |
| — | `OKG_VIZ_LOG` | `INFO` | Log level (read API) |

Vite dev server (development only):

| Env | Purpose |
|---|---|
| `OKG_OPERATOR_API_BASE` | Read-API URL the dev/preview server proxies `/viz/api` to |
| `OKG_OPERATOR_API_TOKEN` | Token the proxy sends as a Bearer header |

## Access And Safety

The UI is read-only, with no restart, publish, repair, source-sync, retry,
approve, or edit controls. A failed read shows its route, view, generation, and
parameters, never an empty default.

`okg ui` **never binds a non-loopback address**; reach it through an SSH
tunnel. The bare read API also carries legacy admin routes: it gates and
audits them with `OKG_VIZ_OPERATOR_TOKEN`, restricts them to loopback peers
without it, and refuses a non-loopback bind without both that token and
`--auth-token-env`. See [Security](../reference/security.md).

On a shared host loopback is not private; run
`export OKG_UI_TOKEN=$(openssl rand -hex 32)` and pass
`--auth-token-env OKG_UI_TOKEN`; every request then needs the token (browser:
any user name, token as password; scripts: `Authorization: Bearer`). A
`nomos.runtime_enforcement` of `audit` or `enforce` refuses to start without
it. A supervised environment sets `ui.auth_token_env` and a matching
`runtime.credential_refs` entry.

## Using The Views

Every view names the graph version its figures come from. The top bar is the
same on every view: the five views, a record search box, the deployment's name,
a version picker, a theme picker, and **Open chat** when the deployment declares
a chat site. The deployment name is printed, not a switcher: each process
serves one deployment, so run a separate `okg ui` for another. The version
picker changes what Explore and Graph read; Home, Updates and Sources always
read the version the read API is serving and say so under their title.

A read that fails says so in place: which read, its status and reason, and a
**Read again** control. The page retries it on its own after 5, 15 and 45
seconds.

### Home

The landing view. It names the deployment and its health (for example
**Degraded**), then the number of records, relationships, record types and
configured sources. **Needs attention** lists what is wrong, worst first --
blocked or overdue sources, enrichers that have produced nothing, runtime
checks such as a missing background worker -- each with its state in words.
**Updates** charts update attempts by outcome per day over 7, 14, 30 or 90
days. **Copy as text** copies the page for an issue or a chat.

### Updates

One row per update attempt, newest first, with its outcome (published, blocked
or running). A blocked attempt shows the reason its checkpoint recorded and the
sources the publication gate excluded. Opening an attempt shows what that
version holds, what the update added, updated and retracted, the checks that
ran and the producer runs recorded against it. See
[blocked updates](../runbooks/troubleshooting/generation-blocked.md).

### Sources

One table of every producer -- sources, enrichers and views -- with its state
in words (Blocked, Overdue, Fresh, Wired but silent, and so on), its last run,
its freshness and the records and relationships it contributed. Chips above the
table count each kind and state and filter the table; a box filters by name.
Opening a producer shows its recent runs.

### Explore

A search box over every record that stores text, plus one chip per record type
with its count. A result opens the record on its **Content** tab, with its
fields, its related records and its provenance on the other tabs.

### Graph

Three ways to look at the same version: **Map** (one circle per producer, sized
by records, linked by relationships), **Table** (every record type with its
producers and relationship counts) and **Grid**. The panel beside the map
lists the biggest relationships and the record types. Picking a record from the
search box opens its neighbourhood.

## Verifying The UI

- **Tests / types:** `npm run operator-ui:test`, `npm run operator-ui:typecheck`.
- **Layout rule:** `npm run operator-ui:layout-check`, also run by the unit
  suite (known top-level directories only; console code under its six role
  directories, test data in `__fixtures__/`, only tests at the console top
  level; a route may not import another route).
- **Rendered proof:** each UI change records browser screenshots and a
  console-error check against a real published deployment in its PACT, and the
  operator signs them off.
- Agent-driven UI changes follow the agent UI workflow
  (`docs/runbooks/agents/ui.md` in the repository: reference pinning,
  browser proof, visual sign-off).

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `okg ui` exits with `operator_ui_bundle_missing` | No built frontend at the path it names. Run `npm install` and `npm run operator-ui:build` at the checkout root, or pass `--dist <dir>`. |
| A request answers `405` with `operator_ui_read_only` | `okg ui` serves reads only. Change deployment state with the CLI, not the UI. |
| Browser shows an `archived` JSON at `/` or `/viz/` | You opened a bare read API (`python -m okg.viz`). Use `okg ui`, or the dev frontend (`:5173`), which proxies `/viz/api` to it. |
| `/viz/api` reads fail with `"no OKG API target"` | Frontend started without `OKG_OPERATOR_API_BASE`. Set it to the read-API URL and reload. |
| `okg ui` exits with `operator_ui_bind_refused` | You asked for a non-loopback host or an unusable port. Bind `127.0.0.1` with a port from 1 to 65535, and use `ssh -L 8765:127.0.0.1:8765 <host>` for remote access. |
| The bare read API refuses a non-loopback bind | Set `OKG_VIZ_OPERATOR_TOKEN` and `--auth-token-env`, or bind `127.0.0.1`. |
| A view renders a failure card with a route + next check | A read failed; this is intentional (no silent defaults). Confirm the deployment is published and readable with the [operating guide](index.md). |
| A read is unexpectedly slow | See the [observability reference](../reference/observability.md) for the slow request and its data source. |

## Related Pages

- [Operator UI contract](../develop/ui/operator-contract.md) — the
  per-view data and behavior specification.
- Agent UI workflow (`docs/runbooks/agents/ui.md` in the repository) — the
  process for changing operator UI surfaces.
- [Run AKMON](index.md) and [CLI reference](../reference/cli.md)
  for the rest of the operator surface.
