---
audience: developer
type: reference
canonical_id: operator-ui-single-app-contract
section: development
visibility: primary
status: active
owner: operator-ui
---
# AKMON Operator UI contract

The AKMON Operator UI is one read-only app for inspecting a deployment and its
OKG graph, and that app is the console: every URL opens it. A `#console/<view>`
hash opens that view; any other hash, including `#home` and `#health` from the
two pages the console replaced, opens the console's Home. Its views, reads and
states are the [AKMON operator console contract](operator-console.md).

## Read-only boundary

The app only issues `GET` requests. There are no write actions.

- No view can change records, relationships, or publication state.
- The app has no restart, repair, publish, source-sync, or queue mutation
  controls.

Missing, stale, unreadable, capped, or failed data is shown explicitly, never
replaced with an empty or healthy default.

## Backend reads

The console's reads are listed on its page. `GET /viz/api/health`,
`GET /viz/api/traces` and `GET /viz/api/workflows` are still served, but no view
reads them since the old Home and Health pages were deleted.

Every response identifies its deployment and generation or states why that
identity is unavailable. Capped responses state what was omitted and why.

The system-status `live_graph` block identifies whether counts came from the
pinned Chronos generation. If that read fails, compatibility-table counts may
still be shown, but only with `status: degraded`, `freshness: unavailable`, and
`source: current_graph_tables`; the omitted pinned count work and generation
scope remain in the payload.

Hierarchy and absorption semantics never interpret catalog-compose or subtype
count failures as an empty catalog. Those API requests return HTTP 503 with a
typed `catalog_semantics_unavailable` body (`cause`, `omitted_work`, deployment
and generation `scope`, retryability, and freshness), which the operator UI
renders as an error state. A corrected retry computes and caches the real
semantics.

## Chat links

No view a chat link is routed to shows a graph record, an agent session, a
producer run or an MCP trace, so a chat answer emits no console link at all:
each identifier renders as plain text with a stated reason. A link to a removed
view would open Home and read as though the answer were wrong. When a view for one of these objects
lands, it routes through `src/okg/substrate/chat/deep_links.py` and nowhere
else.

## Routing

`/viz/api/*` is the read API namespace. The frontend uses one deployment-owned
mount point. Opening the API service at `/` or `/viz/` returns `410`; it is not
the frontend. Unknown `/viz/api/*` paths behave like missing API routes.

Mutating administrative API routes are outside the UI contract and are never
called by the frontend.

## Verification

Changes to this contract require:

- a test proving the app issues only `GET` requests;
- unit coverage for every view, including loud failure and capped-data
  states;
- a browser proof against a real published deployment at desktop and phone
  widths, with a console-error check and the operator's sign-off; and
- the layout rule (`npm run operator-ui:layout-check`, also run by the unit
  suite): the source root holds only its six known directories; console code
  sits under `views/`, `model/`, `api/`, `components/`, `viewers/` or
  `styles/`, with test data in `__fixtures__/` and only test files at the
  console top level; a route may not import another route; and `shared/` may
  not import `app/` or `routes/`.

Static fixtures prove deterministic states. At least one live published
generation proves the same paths against current backend responses.
