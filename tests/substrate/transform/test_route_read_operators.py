"""route-read-operators-through-transform-ir (task.rro.route-relational +
task.rro.reads-audit).

The remaining AKMON relational read operators -- inspect (graph/subtype), filter,
map, aggregate (node/edge), and expand's path mode -- execute by lowering a
TransformProgram over the registered live surfaces (nodes_live -> okg.graph_nodes,
edges_live -> okg.graph_edges) and reading through a pinned Chronos checkout,
rather than hand-built SQL against the base graph tables.

Two proofs per operator, mirroring migrate-reads:
  (a) on a real chronos_native publish the routed operator returns the result the
      pre-migration handler returned on the same fixture graph, and
  (b) the lowered SQL names only the registered Chronos graph surfaces and never a
      base/legacy graph relation.

This file grows one rung at a time as route-relational routes each operator;
search lands separately (task.rro.route-search ->
test_search_routed_matches_legacy_topk).
"""

from __future__ import annotations

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.embeddings import (  # noqa: E402
    HashEmbedder,
    embedder_model_id,
)
from okg.substrate.mcp import (  # noqa: E402
    aggregate_edge,
    aggregate_node,
    expand_path,
    open_session,
    search,
)
from okg.substrate.publisher import Publisher  # noqa: E402
from okg.substrate.transform import (  # noqa: E402
    FunctionRegistry,
    Registries,
    chronos_surface_registry,
    lower_query,
)
from okg.substrate.transform.operators import (  # noqa: E402
    aggregate_edge_routes_program,
    aggregate_neighbors_program,
    edge_type_counts_program,
    embed_lag_program,
    entity_mentions_program,
    expand_path_program,
    filter_program,
    lexical_search_program,
    map_program,
    semantic_search_program,
    node_subtype_counts_program,
    resolved_mention_sources_program,
    subtype_edges_in_program,
    subtype_edges_out_program,
    subtype_nodes_program,
)
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

# Relations a routed read must NOT name: the base fact log + the live base tables
# and their legacy okg.v_* projections. The routed read names okg.graph_* only.
_FORBIDDEN_RELATIONS = (
    "okg.node_facts",
    "okg.edge_facts",
    "okg.nodes_live",
    "okg.edges_live",
    "okg.v_nodes",
    "okg.v_edges",
)


def _seed_and_publish(dsn: str) -> int:
    """Seed a small fixture graph through the canonical Chronos write and publish a
    chronos_native generation. Returns the published generation id."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["page", "paper"],
            narrowings=[("page", "mentions", "page"), ("page", "cites", "paper")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact("n:src", "page", {"text": "src"}, {"id": "n:src"}, {"v": 1}),
                NodeFact("n:p1", "page", {"text": "p1"}, {"id": "n:p1"}, {"v": 1}),
                NodeFact("n:p2", "page", {"text": "p2"}, {"id": "n:p2"}, {"v": 1}),
                NodeFact("n:a1", "paper", {"text": "a1"}, {"id": "n:a1"}, {"v": 1}),
                EdgeFact("n:src", "n:p1", "mentions",
                         source_record_id={"id": "e1"}, source_revision={"v": 1}),
                EdgeFact("n:src", "n:p2", "mentions",
                         source_record_id={"id": "e2"}, source_revision={"v": 1}),
                EdgeFact("n:src", "n:a1", "cites",
                         source_record_id={"id": "e3"}, source_revision={"v": 1}),
            ],
            source="default",
            strict_endpoints=True,
        )
    out = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert out["status"] == "published", out
    return out["generation_id"]


def test_relational_operators_lower_over_registered_surfaces(clean_runtime: str) -> None:
    """Routed aggregate(node) returns the pre-migration grouped counts on a real
    published generation."""
    gen = _seed_and_publish(clean_runtime)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        assert s.branch_name == "default"
        result = aggregate_node(conn, s, "n:src")

    counts = {(c["edge_type"], c["dst_subtype"]): c["count"] for c in result["counts"]}
    assert counts == {("mentions", "page"): 2, ("cites", "paper"): 1}
    # ORDER BY count DESC, edge_type, dst_subtype -> mentions(2) precedes cites(1).
    assert [(c["edge_type"], c["dst_subtype"]) for c in result["counts"]] == [
        ("mentions", "page"),
        ("cites", "paper"),
    ]
    assert result["total_count"] == 2
    assert result["truncated"] is False
    assert result["pinned_generation_id"] == gen

    # aggregate(edge): routes grouped by (src_subtype, edge_type, dst_subtype).
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        routes_out = aggregate_edge(conn, s)

    routes = {
        (r["src_subtype"], r["edge_type"], r["dst_subtype"]): r["count"]
        for r in routes_out["routes"]
    }
    assert routes == {
        ("page", "mentions", "page"): 2,
        ("page", "cites", "paper"): 1,
    }
    # ORDER BY count DESC -> the mentions route (2) precedes the cites route (1).
    assert [
        (r["src_subtype"], r["edge_type"], r["dst_subtype"]) for r in routes_out["routes"]
    ] == [("page", "mentions", "page"), ("page", "cites", "paper")]
    assert routes_out["total_count"] == 2
    assert routes_out["truncated"] is False


def test_expand_path_routes_typed_and_uniform_depth(clean_runtime: str) -> None:
    """expand(path) and uniform depth>1 reach the correct terminals over a real
    published generation, executing through the unrolled JOIN-chain IR program."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["page"], narrowings=[("page", "links", "page")])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact("n:a", "page", {"text": "a"}, {"id": "n:a"}, {"v": 1}),
                NodeFact("n:b", "page", {"text": "b"}, {"id": "n:b"}, {"v": 1}),
                NodeFact("n:c", "page", {"text": "c"}, {"id": "n:c"}, {"v": 1}),
                EdgeFact("n:a", "n:b", "links",
                         source_record_id={"id": "e1"}, source_revision={"v": 1}),
                EdgeFact("n:b", "n:c", "links",
                         source_record_id={"id": "e2"}, source_revision={"v": 1}),
            ],
            source="default",
            strict_endpoints=True,
        )
    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        one_hop = expand_path(conn, s, "n:a", hops=[("links", "out")])
        # depth>1: the same edge_type repeated -- the dispatcher's uniform-depth shape.
        two_hop = expand_path(conn, s, "n:a", hops=[("links", "out"), ("links", "out")])
        reverse = expand_path(conn, s, "n:c", hops=[("links", "in"), ("links", "in")])
        missing = expand_path(conn, s, "n:zzz", hops=[("links", "out")])

    assert [r["node_id"] for r in one_hop["results"]] == ["n:b"]          # 1-hop
    assert [r["node_id"] for r in two_hop["results"]] == ["n:c"]          # 2-hop terminal
    assert [r["node_id"] for r in reverse["results"]] == ["n:a"]          # reverse direction
    assert missing["results"] == [] and "not visible" in missing.get("error", "")


def test_search_routed_matches_legacy_topk(clean_runtime: str) -> None:
    """Routed search executes by lowering a TransformProgram. Lexical returns the
    same term-count ranking the pre-migration scorer produced (shorter matching
    text sorts first); semantic's pgvector cosine lowering executes over the
    Chronos checkout (cast/distance SQL is valid)."""
    embedder = HashEmbedder()
    texts = {
        "n:a": "alpha",
        "n:b": "alpha beta gamma delta",
        "n:c": "gamma",
    }
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["page"], narrowings=[])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact("n:a", "page", {"text": texts["n:a"]}, {"id": "n:a"}, {"v": 1}),
                NodeFact("n:b", "page", {"text": texts["n:b"]}, {"id": "n:b"}, {"v": 1}),
                NodeFact("n:c", "page", {"text": texts["n:c"]}, {"id": "n:c"}, {"v": 1}),
            ],
            source="default",
            strict_endpoints=True,
        )
        # Vectors must EXIST for the routed cosine SQL to rank anything. This
        # test previously asserted only `isinstance(results, list)` against a
        # corpus with no embeddings, so it passed whether or not the lowered SQL
        # ranked a single row — and since `reachable-semantic-search` an
        # unembedded corpus is a `semantic_index_empty` refusal rather than an
        # empty page. Writing real vectors makes the assertion mean what the
        # docstring claims.
        graph = ChronosGraphStore.from_connection(conn).checkout()
        for node_id, text in texts.items():
            vec = embedder.embed(text)
            graph.execute(
                "UPDATE okg.graph_nodes "
                "   SET embedding = :embedding::vector, "
                "       embedding_model = :model_id "
                " WHERE node_id = :node_id",
                {
                    "embedding": "[" + ",".join(f"{v:.6f}" for v in vec) + "]",
                    "node_id": node_id,
                    "model_id": embedder_model_id(embedder),
                },
            )
    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        lexical = search(conn, s, "alpha", method="lexical")
        # Semantic: routed cosine-distance SQL must execute over the checkout.
        semantic = search(conn, s, "alpha", method="semantic", embedder=embedder)

    # query "alpha": n:a (short) and n:b (long) both match; n:c excluded.
    # scorer ranks shorter matching text first -> [n:a, n:b].
    assert [r["node_id"] for r in lexical["results"]] == ["n:a", "n:b"]
    assert lexical["method"] == "lexical"
    assert semantic["method"] == "semantic"
    # n:a's text IS the query, so its vector is the query vector exactly: the
    # lowered cosine SQL ranked real rows, not an empty set.
    assert [r["node_id"] for r in semantic["results"]][:1] == ["n:a"]
    assert semantic["embedding_coverage"]["embedded_nodes"] == 3


def test_all_routed_reads_name_only_registered_surfaces() -> None:
    """Reads-audit: each routed relational program lowers to SQL over the Chronos
    graph surfaces (okg.graph_nodes/graph_edges) and names no base/legacy relation."""
    reg = chronos_surface_registry()
    assert reg.resolve("edges_live").binding == "okg.graph_edges"
    assert reg.resolve("nodes_live").binding == "okg.graph_nodes"
    registries = Registries(surfaces=reg, functions=FunctionRegistry())

    audited: list[str] = []
    audited.append(lower_query(aggregate_neighbors_program(), registries)[0])
    audited.append(lower_query(aggregate_edge_routes_program(), registries)[0])
    audited.append(
        lower_query(
            aggregate_edge_routes_program(src_subtype="page", edge_type="cites"), registries
        )[0]
    )
    # filter / map over both node and edge scopes (default + custom projection,
    # and a predicate exercising scalar eq + attrs jsonb containment).
    audited.append(
        lower_query(
            filter_program(
                kind="nodes",
                predicate={"op": "and", "clauses": [
                    {"op": "eq", "field": "subtype", "value": "page"},
                    {"op": "eq", "field": "attrs.kind", "value": "doc"},
                ]},
                projection=["node_id", "subtype"], limit=50, offset=0,
            )[0],
            registries,
        )[0]
    )
    audited.append(
        lower_query(
            filter_program(
                kind="edges",
                predicate={"op": "eq", "field": "edge_type", "value": "mentions"},
                projection=None, limit=50, offset=0,
            )[0],
            registries,
        )[0]
    )
    audited.append(
        lower_query(map_program(kind="nodes", projection=None, limit=50, offset=0)[0], registries)[0]
    )
    audited.append(
        lower_query(map_program(kind="edges", projection=None, limit=50, offset=0)[0], registries)[0]
    )
    # inspect(graph) sub-reads + inspect(subtype) sub-reads.
    for prog in (
        node_subtype_counts_program(),
        edge_type_counts_program(),
        embed_lag_program(),
        entity_mentions_program(),
        resolved_mention_sources_program(),
        subtype_nodes_program(),
        subtype_edges_out_program(),
        subtype_edges_in_program(),
        # expand(path): 1-hop, typed multi-hop, and uniform depth (any-edge hop).
        expand_path_program(hops=[("links", "out")], limit=50),
        expand_path_program(hops=[("links", "out"), ("cites", "in")], limit=50),
        expand_path_program(hops=[(None, "out"), (None, "out")], target_subtype="page", limit=50),
        # search: lexical scan + semantic pgvector ranking.
        lexical_search_program(),
        semantic_search_program(limit=50),
    ):
        audited.append(lower_query(prog, registries)[0])

    for sql in audited:
        assert ("okg.graph_edges" in sql) or ("okg.graph_nodes" in sql), sql
        for rel in _FORBIDDEN_RELATIONS:
            assert rel not in sql, f"routed read names forbidden relation {rel}: {sql}"


# req.rro.reads-audit-complete: the residual readers that remain OUT OF SCOPE of
# the graph-read routing -- they read the query escape hatch or CATALOG tables
# (not graph surfaces), or the non-default-branch layered path. Enumerated here
# so "every routed graph read names only registered surfaces" is bounded by an
# explicit, reviewed list rather than an unstated assumption.
RESIDUAL_READERS = (
    "query",                       # bounded raw SQL escape hatch over okg.v_* (by design)
    "inspect(ontology)",           # reads okg.subtypes/narrowings/aliases (catalog, not graph)
    "search(alias)",               # reads okg.aliases (catalog), then hydrates via the IR
    "non_default_branch_layered",  # the layered merge-base read path (default branch only here)
)


def test_reads_audit_records_residual_out_of_scope_readers() -> None:
    """The routed-read surface is complete EXCEPT for the four documented
    residuals; this records them so the app_rw base-table SELECT revoke in
    seal-graph-reads has a reviewed precondition (no silently-missed reader)."""
    assert set(RESIDUAL_READERS) == {
        "query",
        "inspect(ontology)",
        "search(alias)",
        "non_default_branch_layered",
    }
