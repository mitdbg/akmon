---
audience: operator, data owner
type: how-to
canonical_id: operations
section: operations
visibility: primary
status: active
owner: documentation
---
# Run it as a service

This section turns a working deployment into a service people and agents rely on.
Start from a deployment such as the one the [Quickstart](../start/quickstart.md) builds. Below, `<name>` is the deployment's name.

1. **Plan.** [Plan a deployment](plan.md): requirements, where it runs, and
   who owns access, credentials, backups and upgrades.
2. **Keep it current.** Run `okg ingest --deployment <name>` after your data
   changes, or let the [background worker](deployments/runtime.md) check
   sources every 5 minutes.
3. **Share it with agents.** Run one MCP server over HTTP with a token; see
   [Share one server over HTTP](../use/agents/index.md#share-one-server-over-http).
4. **Give each person their own access.** [Multi-user HTTP
   principals](../reference/mcp.md#multi-user-http-principals) and
   [access policy](../runbooks/security/nomos.md).
5. **Ask live systems at read time** without importing them:
   [Live relations](live-relations.md).
6. **Watch it.** The commands below, or the read-only
   [operator UI](operator-ui.md).
7. **Back up and upgrade.** [Back up, upgrade, restore,
   retire](lifecycle.md). On Kubernetes, see [Kubernetes](deployments/kubernetes.md).

## Watch it

```bash
okg status --deployment <name>
okg doctor --deployment <name>
```

`okg status` shows, under `publishing`, the version readers see
(`latest_published_generation`), whether it published, and its record and
link counts. `okg doctor` checks the configuration, database, sources and,
when it runs, the worker; it exits 1 and names a fix when something is
wrong. A search always reads the last published version, so a failed import
or publish never takes the graph down: readers keep the previous version until a new one
publishes.

## When something fails

If a command fails, it names the error and a fix; start from the row that
matches what you see.

| What you see | Start here |
| --- | --- |
| The first install or import fails | [First-run triage](../runbooks/troubleshooting/first-run.md) |
| A new version is refused (`blocked`) | [Publication keeps failing](../runbooks/troubleshooting/generation-blocked.md) |
| A source stopped updating | [Source stopped updating](../runbooks/troubleshooting/source-sync.md) |
| The worker's queue is stuck | [Runtime queue recovery](../runbooks/troubleshooting/runtime-queue.md) |
| Disk use grows | [Storage and history](../runbooks/troubleshooting/storage.md) |

[All runbooks](../runbooks/index.md) cover the rest. Record the deployment,
the failing signal and the actual output before you change configuration or
retry work.

## Responsibilities

Operators need named owners for source access and credential rotation, graph
rules and access policy, the worker, storage, backups, upgrades, and
escalation to the data owner. A backup that has not been restored is
unproven.
