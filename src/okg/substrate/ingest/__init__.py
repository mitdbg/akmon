"""Substrate ingest runner (M6.7.1).

Closes the integration gap identified in deep review: adapters
from `okg.substrate.library.sources` yield NodeFact / EdgeFact
objects, but nothing wrote them to `okg.node_facts` /
`okg.edge_facts`. `SourceRunner` is that bridge.

Usage:

    from okg.substrate.library.sources.docsite import DocsiteSource
    from okg.substrate.ingest import SourceRunner

    adapter = DocsiteSource(fixture_path="deployments/foo/docs.json")
    runner = SourceRunner(dsn, source_name="docs")
    result = runner.run(adapter, mode="cursor")
    # → result.nodes_appended / edges_appended / run_id

The runner is the ONLY path from an adapter's facts to the
substrate. Bootstrap has its own inline COPY (by design, for
large-snapshot performance); that path remains.
"""
from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from okg.substrate.ingest.planning import (
        ActivationProbeResult,
        IngestPlan,
        plan_ingest_sources,
        resolve_predecessor_deps,
        topo_layers,
        topo_sort,
    )
    from okg.substrate.ingest.runner import RunResult, SourceRunner, SourceRunContext
    from okg.substrate.ingest.service import IngestOptions, IngestRunResult, run_ingest


_EXPORTS = {
    "ActivationProbeResult": "okg.substrate.ingest.planning",
    "IngestOptions": "okg.substrate.ingest.service",
    "IngestPlan": "okg.substrate.ingest.planning",
    "IngestRunResult": "okg.substrate.ingest.service",
    "plan_ingest_sources": "okg.substrate.ingest.planning",
    "resolve_predecessor_deps": "okg.substrate.ingest.planning",
    "RunResult": "okg.substrate.ingest.runner",
    "run_ingest": "okg.substrate.ingest.service",
    "SourceRunner": "okg.substrate.ingest.runner",
    "SourceRunContext": "okg.substrate.ingest.runner",
    "topo_layers": "okg.substrate.ingest.planning",
    "topo_sort": "okg.substrate.ingest.planning",
}


__all__ = [
    "ActivationProbeResult",
    "IngestOptions",
    "IngestPlan",
    "IngestRunResult",
    "plan_ingest_sources",
    "RunResult",
    "SourceRunner",
    "SourceRunContext",
    "resolve_predecessor_deps",
    "run_ingest",
    "topo_layers",
    "topo_sort",
]


def __getattr__(name: str) -> Any:
    """Load compatibility exports without making pure leaves import effects."""

    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Expose lazy compatibility names to API introspection."""

    return sorted(set(globals()) | set(__all__))
