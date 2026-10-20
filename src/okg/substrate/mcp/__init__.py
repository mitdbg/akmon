"""Generation-pinned MCP read tools (M4).

Substrate-side tool implementations that read the Chronos graph
(`okg.graph_nodes`, `okg.graph_edges`) through a checkout pinned to
a `generation_id`. Agents hold a `Session` for the duration of a
multi-step reasoning sequence; every tool call on that session
sees the graph as it was at the session's pinned generation,
even while new generations publish concurrently.

Per design.md "Generation-pinned reads" + the proposal's A4
decision (primary-only reads in v1; replica routing is a
follow-up).

The agent-facing graph-traversal surface is locked at 8 tools
plus `run_sql` per `retire-redundant-mcp-tools` (2026-04-27).
Retired analytical/metadata queries ship as documented recipes
in `docs/reference/mcp.md`.
"""
from okg.substrate.mcp.session import (
    Session,
    advance_session,
    assert_session_matches,
    open_session,
    preview_advance,
    resolve_generation,
)
from okg.substrate.mcp.tools import (
    aggregate_edge,
    aggregate_node,
    expand_neighbors,
    expand_path,
    find_identifier_mentions,
    get_ontology,
    inspect_attr_provenance,
    inspect_generation,
    inspect_graph,
    inspect_node,
    inspect_subtype,
    inspect_view,
    parse_path_hops,
)

# `search` is bound LAST on purpose (issue #2212). Every other
# `okg.substrate.mcp` submodule this package imports is fully loaded
# before `search.py` starts executing, so no import triggered from inside
# `search.py` can re-enter this package and resolve
# `from okg.substrate.mcp.search import search` against a partially
# initialized module. Keep this import last: the guard in
# tests/substrate/mcp/test_search_package_init_import_order.py refuses any
# `okg.substrate.mcp.*` import placed after it.
from okg.substrate.mcp.search import search  # isort: skip


__all__ = [
    "Session",
    "advance_session",
    "aggregate_edge",
    "aggregate_node",
    "assert_session_matches",
    "expand_neighbors",
    "expand_path",
    "find_identifier_mentions",
    "get_ontology",
    "inspect_attr_provenance",
    "inspect_generation",
    "inspect_graph",
    "inspect_node",
    "inspect_subtype",
    "inspect_view",
    "open_session",
    "parse_path_hops",
    "preview_advance",
    "resolve_generation",
    "search",
]
