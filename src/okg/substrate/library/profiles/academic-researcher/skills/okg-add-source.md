---
name: okg-add-source
description: Expand or reconfigure academic-researcher OKG sources through the single setup planner. Triggers on "add gmail", "ingest my calendar", "wire up slack", "pull in my CV", "add another source", "extend ingestion".
---

# OKG Source Expansion

## Goal

Bring additional sources online without creating a second setup path.
Use the same planner and committer as first setup, then verify emitted
graph rows.

## Required Data

- **Deployment**: the academic-researcher deployment slug.
- **Scope**: source names or narrowing inputs such as project paths,
  notes directories, paper inboxes, calendars, labels, channels, or
  repos.
- **Allowlists**: required for broad/private sources before ingest.

## Operator Surface

Use MCP tools, not source-specific terminal instructions:

1. Inspect current wiring with `list_sources(deployment)`.
2. Build the source plan with `propose_setup(deployment, only=[...])`
   or source-specific narrowing arguments such as `project_paths`.
3. For credentialed apps, inspect route readiness with
   `connector_route_status(deployment, source="gmail")`,
   `connector_route_status(deployment, source="google-cal")`, or
   `connector_route_status(deployment, source="slack")`.
4. Commit approved local/public/path sources with
   `commit_setup(deployment, approved=[...])`.
5. When a host/provider/gateway connector returns bounded normalized
   records, import them with `connector_ingest_batch(...)`.
6. Verify with `source_status(deployment, short_name)` and graph
   inspection tools such as `search(...)` or `aggregate(...)`.

`add_source(...)` is a low-level recovery hook for explicit local paths
or URLs after the operator has already supplied the exact value. Do not
use it as the normal setup product surface.

## Built-In Connector Sources

- **Gmail**: connector route only. Requires labels or query/time-range
  allowlists before import. Data arrives as bounded normalized email and
  thread records.
- **Google Calendar**: connector route only for private Google calendars.
  Requires calendar allowlists. Public or exported `.ics` feeds stay on
  the local URL path through `calendar_ics`.
- **Slack**: connector route only. Requires channel allowlists. Prefer a
  host/provider/gateway route that is already connected and read-only.

## Output Shape

Return three blocks:

1. **Plan**: source id, route kind, setup state, and required allowlists.
2. **Commit/import result**: source id, nodes added, edges added, and
   any no-row or failure hint.
3. **Sample**: a few node ids or headlines from the new source.

## Anti-Patterns

- Do not route Gmail, Google Calendar, or Slack through per-source CLI
  setup. They are connector-readiness entries until a trusted read-only
  route and allowlist exist.
- Do not hand-edit `source_registry.yaml` for operator setup. Use the
  planner and committer so discovery, dry-run, and envelope handling stay
  consistent.
- Do not configure broad private sources without allowlists.
- Do not run a full ingest after adding one source unless the operator
  explicitly asks for a full refresh.
