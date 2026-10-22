---
audience: user, data owner, operator, developer
type: explanation
canonical_id: docs-site-home
section: start
visibility: primary
status: active
owner: documentation
---
# AKMON documentation

AKMON turns a team's own sources into a versioned, checked knowledge graph
that people and coding agents can query, with every answer traceable to its
source and graph version.

You run it yourself, against a PostgreSQL database you control. It is not a
hosted service. The software is installed as the Python package `okg`, and
its command is also `okg`. OKG is the name of the graph that AKMON manages.

## Who this is for

- **Teams building a graph of their own domain**: wikis, code, tickets and
  catalogs, connected in one graph their people and agents can search.
- **Operators** who run that graph as a shared service.
- **Developers** who want a graph of one repository for their coding agent.
- **Researchers** who want one graph over their papers, code and notes.

## Four terms used everywhere

- A **deployment** is one configured graph: its database, its sources, and
  its rules. See [Deployment](reference/glossary.md#deployment).
- A **source** is one body of data AKMON may read, such as a folder or a
  repository. Reading it is an **import**.
- A **published version** is a complete, checked snapshot of the graph.
  Commands and results call it a **generation** and identify it by
  `generation_id`. Searches read a published version, never half-finished
  work. See [Generation](reference/glossary.md#generation).
- **Provenance** is the record of where a result came from: the source, the
  source's own identifier, and the import that produced it.

The [glossary](reference/glossary.md) defines the rest.

## Get started

- [Quickstart](start/quickstart.md): in about 15 minutes after the database
  image downloads (about 1.9 GB to download, 6.5 GB on disk), install a released version,
  build a graph from one of your repositories and a folder of your documents,
  connect your agent, and get a cited answer. You need Git, `uv`, and Docker
  or podman.
- [The AKMON model](start/concepts.md) explains deployments, sources,
  published versions and provenance.
- [How information becomes searchable](start/data-lifecycle.md) follows one
  change from import to a searchable published version.
- [Install from a source checkout](operate/install.md) builds the same first
  graph from a source checkout instead of a release: where its configuration lives, a second
  graph, and Codex logs. [Install from a wheel](operate/install-packaged.md)
  covers a wheel or a producer-built release.
- Optional, later: [Evaluate AKMON from source](start/evaluate-from-source.md)
  builds a small throwaway graph from two sample records, publishes it and
  searches it.

## Connect your data

- [Connect your data](use/sources/index.md): choose a route, then add,
  refresh and remove a source. Each built-in type has its own page:
  [local folder](use/sources/local-folder.md) and
  [GitHub](use/sources/github.md).
- [Bring your own data with a manifest](use/sources/manifest.md): import
  your own records, with their own types and links, from one YAML file; every
  key is in the [Source manifest reference](reference/source-manifest.md).
- [Configure a deployment](operate/configure.md) and
  [choose a profile](use/profiles/index.md).

## Connect agents

- [Connect an agent](use/agents/index.md): connect any MCP-capable agent
  (MCP, the Model Context Protocol, is the standard way agents call tools),
  with one command for Claude Code, Claude Desktop, Cursor and Codex, or
  share one server over HTTP.
- [Query and trace the graph](use/search/index.md): search, follow
  relationships and read the evidence.
- [Set up chat](operate/chat.md): a browser chat that answers from the graph.

## Run it as a service

- [Run it as a service](operate/index.md): the path from a working graph to
  a shared service, how to watch it, and where to start when something fails.
- [Plan a deployment](operate/plan.md) before connecting private data.
- [Keep the graph current](operate/deployments/runtime.md) with the
  background worker, or [deploy on Kubernetes](operate/deployments/kubernetes.md).
- [Back up, upgrade, restore and retire](operate/lifecycle.md) a deployment.

## Reference

| Need | Reference |
| --- | --- |
| Commands and their options | [Command line](reference/cli.md) |
| Graph reads for agents and programs (MCP) | [MCP graph reads](reference/mcp.md) |
| Python functions and result types | [SDK API](reference/sdk-api.md) |
| Built-in source readers, profiles, and graph modules | [Capabilities](reference/capabilities.md) |
| Credentials and access boundaries | [Security](reference/security.md) |
| Status fields and metrics | [Observability](reference/observability.md) |
| Tables and keys | [Database schema](reference/database-schema.md) |
| Terms | [Glossary](reference/glossary.md) |

## Develop AKMON

- [System architecture](develop/architecture/overview.md) and
  [code architecture](develop/architecture/pipeline.md).
- [Add a source adapter](develop/index.md#add-a-source-adapter), the code that
  reads a new kind of source.
- [Extend the graph model](develop/extensions/ontologies.md) with new record
  and relationship types.
