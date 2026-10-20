"""Publish a generation with REAL vectors, at chosen model identities (#797).

Vectors are written through the same identity derivation the embed workflow
uses, so a fixture cannot claim a provenance the substrate would not have
recorded. Where a test needs SKEW it says so by naming a different identity
explicitly — never by relying on a literal that happens to differ.
"""
from __future__ import annotations

from typing import Any, Iterable, Optional

import psycopg

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.substrate.mcp.search_fixture import make_search_deployment, node
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


__all__ = ["make_search_deployment", "node", "publish_with_vectors"]


def publish_with_vectors(
    dsn: str,
    deployment: str,
    *,
    subtypes: Iterable[str],
    nodes: list[NodeFact],
    vectors: dict[str, list[float]],
    model_id: Optional[str],
    per_node_model_id: Optional[dict[str, Optional[str]]] = None,
) -> str:
    """Publish `nodes`, giving the named ones a vector and a recorded model.

    `model_id=None` writes a NULL `embedding_model` — the legacy state a
    live migration leaves behind: a real vector whose provenance is unknown.
    `per_node_model_id` overrides it per node, so one generation can hold rows
    from two models (or a mix of known and unknown) without a second publish.

    Nodes absent from `vectors` keep a NULL embedding, which is how a
    partially-embedded corpus is built.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=list(subtypes))
        ChronosGraphStore.from_connection(conn).apply_facts(
            nodes,
            source="system:analyst_search_fixture",
            strict_endpoints=True,
        )
        graph = ChronosGraphStore.from_connection(conn).checkout()
        for node_id, vec in vectors.items():
            row_model = (
                per_node_model_id.get(node_id, model_id)
                if per_node_model_id is not None
                else model_id
            )
            graph.execute(
                "UPDATE okg.graph_nodes "
                "   SET embedding = :embedding::vector, "
                "       embedding_model = :model_id "
                " WHERE node_id = :node_id",
                {
                    "embedding": "[" + ",".join(f"{v:.6f}" for v in vec) + "]",
                    "node_id": node_id,
                    "model_id": row_model,
                },
            )
    result: dict[str, Any] = Publisher(dsn, deployment, catalog_identity=bind_test_catalog_writer(dsn, deployment)).cycle()
    assert result["status"] == "published", result
    return str(result["generation_id"])
