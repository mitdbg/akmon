"""OKG — Operational Knowledge Graph (Postgres-native substrate).

The substrate (`okg.substrate.*`) is the canonical pipeline:
sources → narrowing trigger → projector → enrichers → publisher
→ MCP, all backed by Postgres + TimescaleDB + pgvector.

Read-side helpers:
- `okg.views.*` — view authoring + rendering layer
- `okg.viz.*` — interactive graph visualization HTTP surface
- `okg.extraction` — shared `EntityExtractor` Protocol +
  `ExtractedEntity` dataclass (substrate-wide)

Pre-substrate runtime (`okg.ingest`, `okg.modules`, `okg.audit`,
`okg.simulation`, `okg.core`, `okg.mcp_server`, etc.) was deleted
on the `rearchitect-on-postgres` branch (2026-04-27). Anything
that needs that surface back gets a new substrate-capability
proposal designed in Postgres primitives.
"""


def __getattr__(name: str) -> str:
    """`okg.__version__`: the installed distribution's version.

    The one declaration is `[project] version` in pyproject.toml; this reads it
    back through package metadata, as `okg --version` does, so no second copy
    exists to drift. It is looked up on access rather than at import, and a
    missing install raises instead of reporting a guessed number.
    """
    if name != "__version__":
        raise AttributeError(f"module 'okg' has no attribute {name!r}")
    import importlib.metadata

    try:
        return importlib.metadata.version("okg")
    except importlib.metadata.PackageNotFoundError as error:
        raise AttributeError(
            "okg.__version__ is unknown: no installed package metadata for 'okg'. "
            "Install okg (for example `uv sync`) to report a version."
        ) from error
