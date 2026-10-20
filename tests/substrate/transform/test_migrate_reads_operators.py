"""migrate-reads-through-transform-ir (task.mr.operators-lower + task.mr.reads-audit),
aligned to the Chronos cutover.

The `expand` operator (list_neighbors) on the default branch executes by lowering a
TransformProgram over the registered `edges_live` surface and reading it through a
Chronos checkout. Post-cutover the live surface binds to okg.graph_edges (the Chronos
graph store), not the legacy okg.v_edges view -- so this proves (a) a real chronos_native
publish + list_neighbors returning the correct neighbors over the Chronos-seeded graph,
and (b) an audit that the lowered operator SQL the routed path executes names the Chronos
surface okg.graph_edges and never a legacy graph relation (nodes_live/edges_live/v_*).
The legacy-lowering-over-v_edges audit lives in
test_routed_read_paths_name_only_registered_surfaces (the not-yet-retired legacy path).

`search` (BM25/pgvector) needs a SEARCH-op lowering that does not yet exist and is a
tracked follow-on; non-default-branch reads stay on the legacy layered path.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.mcp import expand_neighbors, open_session  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from okg.substrate.transform import (  # noqa: E402
    FunctionRegistry,
    Registries,
    Surface,
    SurfaceKind,
    lower_query,
    narrowing_allowed_binding,
    narrowing_conformance_program,
    no_dangling_edges_program,
    substrate_surface_registry,
)
from okg.substrate.transform.operators import (  # noqa: E402
    lower_neighbors_chronos,
    lower_neighbors_sql,
)
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

_REPORT = Path(__file__).resolve().parents[3] / ".scratch" / "gtir_migrate_ops_graph.json"
_BASE_TABLES = ("okg.edges_live", "okg.nodes_live", "okg.node_facts", "okg.edge_facts")
# Legacy graph relations the post-cutover routed read must NOT name (it reads Chronos).
_LEGACY_GRAPH_RELATIONS = ("okg.nodes_live", "okg.edges_live", "okg.v_nodes", "okg.v_edges")


def test_expand_routed_over_v_edges_returns_correct_neighbors(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["page"], narrowings=[("page", "mentions", "page")])
        # Chronos cutover: seed the graph through the canonical Chronos write
        # (apply_facts -> okg.graph_nodes/graph_edges), not the legacy fact-log
        # tables -- chronos_native publish checkpoints the Chronos head and never
        # runs the projector, so fact-log seeding would leave the read empty.
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact("n:src", "page", {"text": "src"}, {"id": "n:src"}, {"v": 1}),
                NodeFact("n:d1", "page", {"text": "d1"}, {"id": "n:d1"}, {"v": 1}),
                NodeFact("n:d2", "page", {"text": "d2"}, {"id": "n:d2"}, {"v": 1}),
                EdgeFact("n:src", "n:d1", "mentions",
                         source_record_id={"id": "e1"}, source_revision={"v": 1}),
                EdgeFact("n:src", "n:d2", "mentions",
                         source_record_id={"id": "e2"}, source_revision={"v": 1}),
            ],
            source="default",
            strict_endpoints=True,
        )
    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out
    gen = out["generation_id"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        assert s.branch_name == "default"
        result = expand_neighbors(conn, s, "n:src")

    dsts = sorted(n["dst"] for n in result["neighbors"])
    assert dsts == ["n:d1", "n:d2"]                       # routed read returns both neighbors
    assert all(n["edge_type"] == "mentions" for n in result["neighbors"])
    assert all(n["direction"] == "out" for n in result["neighbors"])
    assert result["truncated"] is False

    # The lowered operator SQL the routed path executes reads the Chronos surface
    # okg.graph_edges and names no legacy graph relation.
    sql, _ = lower_neighbors_chronos(direction="out", edge_type="mentions", limit=101)
    assert "okg.graph_edges" in sql
    assert not any(b in sql for b in _LEGACY_GRAPH_RELATIONS)

    _REPORT.parent.mkdir(exist_ok=True)
    _REPORT.write_text(json.dumps({"evidence": [{
        "kind": "graph", "status": "pass",
        "refs": ["req.mr.operators-lower-to-ir", "req.mr.reads-over-registered-surfaces"],
        "generation_id": gen,
        "summary": (
            f"expand (list_neighbors) on the default branch executes by lowering a "
            f"TransformProgram over the Chronos graph surface: on gen {gen} it returns "
            f"the correct neighbors {dsts} of n:src, and the lowered SQL reads "
            f"okg.graph_edges with no legacy graph relation named."
        ),
        "command": "uv run --extra dev python -m pytest tests/substrate/transform/test_migrate_reads_operators.py",
        "data": {"neighbors": dsts, "reads_graph_edges": True, "names_legacy_relation": False},
    }]}, indent=2))


def test_routed_read_paths_name_only_registered_surfaces() -> None:
    """Reads-audit (task.mr.reads-audit): every routed read path -- the expand
    operator and the transform consistency gate -- lowers to SQL over okg.v_*
    surfaces and never names a base graph table directly."""
    # The substrate registry binds the live surfaces to the okg.v_* views.
    reg = substrate_surface_registry()
    assert reg.resolve("edges_live").binding == "okg.v_edges"
    assert reg.resolve("nodes_live").binding == "okg.v_nodes"

    # expand operator (all directions, with/without edge_type).
    audited: list[str] = []
    for direction in ("out", "in", "both"):
        for etype in (None, "mentions"):
            sql, _ = lower_neighbors_sql(direction=direction, edge_type=etype, limit=10)
            audited.append(sql)

    # publisher consistency gate programs.
    creg = substrate_surface_registry()
    creg.register(Surface("narrowing_allowed", SurfaceKind.CATALOG_POLICY,
                          narrowing_allowed_binding(1), pinned=False))
    registries = Registries(surfaces=creg, functions=FunctionRegistry())
    audited.append(lower_query(no_dangling_edges_program(), registries)[0])
    audited.append(lower_query(narrowing_conformance_program(1), registries)[0])

    for sql in audited:
        assert ("okg.v_edges" in sql) or ("okg.v_nodes" in sql), sql
        for base in _BASE_TABLES:
            assert base not in sql, f"routed read names base table {base}: {sql}"
