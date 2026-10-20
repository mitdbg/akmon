# AKMON

AKMON turns a team's own sources into a versioned, checked knowledge graph
that people and coding agents can query, with every answer traceable to its
source and graph version.

You run AKMON yourself, on a PostgreSQL database you control. It reads your
sources (folders of documents, Git repositories, issue trackers, catalogs and
other systems), checks each import against the record types you declare, and
publishes a complete new graph version only when that version passes your
checks. People search it from the command line; coding agents read it through
the Model Context Protocol (MCP), the standard way agents call tools.

## Who it is for

- **Teams building a graph of their own domain**: their wikis, code, tickets
  and catalogs, connected and searchable in one place.
- **Operators** who run that graph as a shared service.
- **Developers** who want a graph of one repository for their coding agent.
- **Researchers** who want one graph over their papers, code and notes.

## What you get

- **Checked versions.** Each publish builds a full candidate graph and runs
  the deployment's checks. If a check fails, readers stay on the previous
  version.
- **Provenance on every record.** Each record keeps its source, the source's
  own identifier and revision, and the import that wrote it.
- **Your own model.** Each deployment declares the record and relationship
  types it accepts and the sources it may read.
- **Bounded reads for agents.** Each read names the graph version it used and
  says whether its result is complete.

The checks prove that the graph matches your rules. They do not prove that
what a source says is true.

## Start

The [Quickstart](docs/start/quickstart.md) takes about 15 minutes after the
database image downloads (about 6.5 GB the first time). It
builds a graph from one of your Git repositories and a folder of your
documents, connects your coding agent, and ends with a cited answer. You need
Linux or macOS, Git, [`uv`](https://docs.astral.sh/uv/getting-started/installation/)
and Docker.

## Documentation

The [documentation home](docs/README.md) is organized by task:

- [Get started](docs/start/quickstart.md): a first graph and a cited answer,
  then what AKMON is and its model.
- [Connect your data](docs/use/sources/index.md): bring your sources in and
  keep them current, including your own records
  [described in a manifest](docs/use/sources/manifest.md).
- [Connect agents](docs/use/agents/index.md): connect any MCP agent, or
  query from the command line or chat.
- [Run it as a service](docs/operate/plan.md): plan, deploy, secure, monitor,
  back up and upgrade a shared deployment.
- [Reference](docs/reference/cli.md): commands, MCP tools, capabilities and
  terms.
- [Develop AKMON](docs/develop/index.md): architecture, extensions and
  contributing.

## Contributing

Start with [Develop AKMON](docs/develop/index.md) and
[CONTRIBUTING.md](CONTRIBUTING.md). For changes to the import,
publication, or query path, also read the
[code architecture](docs/develop/architecture/pipeline.md).

## License

AKMON is released under the MIT License; see [LICENSE](LICENSE).
