# AKMON

**A consistent, versioned knowledge base of an organization's data, built from
its many sources and read by people and agents.**

The data of a large organization is scattered across many systems: tickets,
documentation, code, monitoring, meetings. It keeps changing, parts of it are
stale or contradict each other, and it is hard to stitch together. AKMON
builds and maintains one knowledge graph of it, the operational knowledge
graph (OKG), so that people, agents and software can query it.

```text
sources ─▶ incremental ingestion ─▶ consistency checks ─▶ versioned graph ─▶ server ─▶ people and agents
```

AKMON runs on PostgreSQL, on your own machines. Agents connect to it over the
Model Context Protocol (MCP); people use the `okg` command.

## Get started

The [Quickstart](docs/start/quickstart.md) takes you from nothing to a graph
of your own repository and documents, with your coding agent connected. You
need Linux or macOS, Git, [`uv`](https://docs.astral.sh/uv/), and Docker or
podman.

## Documentation

- [Get started](docs/start/quickstart.md) and [the AKMON model](docs/start/concepts.md)
- [Connect your data](docs/use/sources/index.md)
- [Connect agents](docs/use/agents/index.md)
- [Run it as a service](docs/operate/plan.md)
- [Reference](docs/reference/cli.md)
- [Develop AKMON](docs/develop/index.md)

## Contributing

Start with [Develop AKMON](docs/develop/index.md) and
[CONTRIBUTING.md](CONTRIBUTING.md). For changes to the import,
publication, or query path, also read the
[code architecture](docs/develop/architecture/pipeline.md).

## License

AKMON is released under the MIT License; see [LICENSE](LICENSE).
