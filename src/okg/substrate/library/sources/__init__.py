"""Built-in source adapters (M5b.6.1).

Each adapter in this package implements the `SourceAdapter`
protocol defined in `base.py`. Adapters yield `NodeFact` and
`EdgeFact` records; the substrate's ingest runner takes care
of batching, dedupe_key minting, and COPY into `okg.node_facts`
/ `okg.edge_facts`.

Shipping fidelity (M5b scope statement):
  - Each of the 9 named adapters ships with a Python class
    honoring the protocol and the fact shape.
  - Every adapter supports a `fixture_path=<file>` mode that
    reads from a local JSON fixture instead of hitting the
    network. This is the ready-to-run path for tests,
    templates, and dev iteration.
  - Production HTTP clients (pagination, rate-limit, auth) are
    marked with a `_fetch_live()` method that raises
    `NotImplementedError("production HTTP client TBD; pass "
    "fixture_path= to run from a local snapshot")`. Per-deployment
    hardening lands when a real deployment needs it.

This matches the legacy deployment pattern: existing
`deployments/*/source_registry.yaml` entries already declare
`fixture:` paths.
"""
from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    SourceAdapter,
    SourceRun,
    RecordEmission,
    RecordNode,
    RecordSet,
    available_adapters,
)


__all__ = [
    "EdgeFact",
    "EdgeKey",
    "NodeFact",
    "RecordEmission",
    "RecordNode",
    "RecordSet",
    "SourceAdapter",
    "SourceRun",
    "available_adapters",
]
