"""Coverage tests for `SemanticConceptLinker`.

Three layers:
1. Constructor validation (no DB needed).
2. enrich() skip-paths when concepts or sections lack embeddings.
3. enrich() happy path with synthetic schema-dim embeddings;
   `okg.find_concept_links` is a real SQL function defined in
   `src/okg/substrate/db/schema.sql:134` so the test exercises it
   end-to-end.
"""
from __future__ import annotations

import logging

import psycopg
import pytest

from okg.substrate.library.enrichers.semantic_concept_linker import (
    SemanticConceptLinker,
)

pytestmark = pytest.mark.postgres

_EMBED_DIM = 2560


def _zero_vec() -> str:
    return "[" + ",".join(["0"] * _EMBED_DIM) + "]"


def _one_hot(index: int) -> str:
    values = ["0"] * _EMBED_DIM
    values[index] = "1"
    return "[" + ",".join(values) + "]"


def _ensure_generation(conn: psycopg.Connection, generation_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT max(catalog_version_id) "
            "FROM okg.catalog_versions WHERE status = 'applied'"
        )
        cv = cur.fetchone()[0] or 1
        cur.execute(
            "INSERT INTO okg.graph_generations "
            "  (generation_id, deployment_name, status, "
            "   catalog_version_id, published_at, correlation_id) "
            "VALUES (%s, 'semantic-linker-test', 'published', "
            "        %s, now(), %s) "
            "ON CONFLICT (generation_id) DO NOTHING",
            (
                generation_id,
                cv,
                f"semantic-linker-test:{generation_id}",
            ),
        )


def _seed_graph_nodes(
    conn: psycopg.Connection,
    rows: list[tuple[str, str, dict, str | None]],
) -> None:
    """Write node rows (with optional pgvector embeddings) into the Chronos
    graph default branch.

    The semantic linker reads `okg.graph_nodes` via a Chronos checkout, so
    seeds must land there rather than in the legacy `okg.nodes_live` table.
    `apply_facts` always stores `embedding=NULL`, so embeddings are written
    here via a direct `upsert_rows` pass mirroring DBOS okg_embed_batch
    output. Each tuple is
    `(node_id, subtype, attrs, embedding_literal_or_None)`.
    """
    from okg.substrate.graph_store import (
        DEFAULT_BRANCH,
        NODE_TABLE,
        ChronosGraphStore,
    )

    store = ChronosGraphStore.from_connection(conn)
    session = store.checkout(DEFAULT_BRANCH)
    with session.transaction():
        session.upsert_rows(
            NODE_TABLE,
            [
                {
                    "node_id": node_id,
                    "subtype": subtype,
                    "attrs": dict(attrs),
                    "text_hash": None,
                    "embedding": embedding,
                    "embedding_model": None,
                    "source": "fixture.semantic-concept-linker",
                    "source_record_id": {"id": node_id},
                    "source_revision": {"v": 1},
                    "run_id": None,
                    "observed_at": None,
                }
                for node_id, subtype, attrs, embedding in rows
            ],
        )


def _checkpoint_generation(
    conn: psycopg.Connection, generation_id: int
) -> None:
    """Snapshot the current Chronos default-branch state as the named
    generation so `checkout_generation(generation_id)` resolves."""
    from okg.substrate.graph_store import ChronosGraphStore

    ChronosGraphStore.from_connection(conn).checkpoint_generation(
        generation_id
    )


# ---------- constructor validation ----------


def test_ctor_requires_source_subtype() -> None:
    with pytest.raises(ValueError, match="source_subtype"):
        SemanticConceptLinker(
            source_subtype="",
            section_subtype="section",
            parent_attr="pub_id",
            min_similarity=0.5,
        )


def test_ctor_requires_section_subtype() -> None:
    with pytest.raises(ValueError, match="section_subtype"):
        SemanticConceptLinker(
            source_subtype="publication",
            section_subtype="",
            parent_attr="pub_id",
            min_similarity=0.5,
        )


def test_ctor_requires_parent_attr() -> None:
    with pytest.raises(ValueError, match="parent_attr"):
        SemanticConceptLinker(
            source_subtype="publication",
            section_subtype="section",
            parent_attr="",
            min_similarity=0.5,
        )


def test_ctor_requires_min_similarity() -> None:
    with pytest.raises(ValueError, match="min_similarity is required"):
        SemanticConceptLinker(
            source_subtype="publication",
            section_subtype="section",
            parent_attr="pub_id",
        )


def test_ctor_rejects_min_similarity_out_of_range() -> None:
    with pytest.raises(ValueError, match="must be a number"):
        SemanticConceptLinker(
            source_subtype="publication",
            section_subtype="section",
            parent_attr="pub_id",
            min_similarity=2.0,
        )


def test_ctor_rejects_non_positive_top_k() -> None:
    with pytest.raises(ValueError, match="top_k_per_concept"):
        SemanticConceptLinker(
            source_subtype="publication",
            section_subtype="section",
            parent_attr="pub_id",
            min_similarity=0.5,
            top_k_per_concept=0,
        )


def test_ctor_happy_path_records_attrs() -> None:
    linker = SemanticConceptLinker(
        source_subtype="publication",
        section_subtype="section",
        parent_attr="pub_id",
        min_similarity=0.5,
        top_k_per_concept=10,
    )
    assert linker.source_subtype == "publication"
    assert linker.section_subtype == "section"
    assert linker.parent_attr == "pub_id"
    assert linker.min_similarity == 0.5
    assert linker.top_k_per_concept == 10
    assert linker.emits_edge_types == ("mentions",)
    # Required for the publisher's narrowing validator.
    assert (
        "publication", "mentions", "concept",
    ) in linker.requires_narrowings


# ---------- enrich() skip paths ----------


def test_enrich_skips_when_no_concepts_have_embeddings(
    clean_runtime: str, caplog,
) -> None:
    """No Concept rows with embedding → emit zero edges + WARNING."""
    linker = SemanticConceptLinker(
        source_subtype="publication",
        section_subtype="section",
        parent_attr="pub_id",
        min_similarity=0.5,
    )
    with caplog.at_level(
        logging.WARNING,
        logger="okg.substrate.library.enrichers.semantic_concept_linker",
    ):
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            _ensure_generation(conn, 1)
            _checkpoint_generation(conn, 1)
            result = linker.enrich(conn, generation_id=1)
    assert result.n_edges_emitted == 0
    assert any(
        "zero Concept nodes have embeddings" in rec.message
        for rec in caplog.records
    )


def test_enrich_skips_when_no_sections_have_embeddings(
    clean_runtime: str, caplog,
) -> None:
    """Concept rows have embeddings but section rows do not → emit
    zero edges + WARNING."""
    # Seed one concept with a fake schema-dim embedding.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _ensure_generation(conn, 1)
        _ensure_generation(conn, 2)
        _ensure_generation(conn, 3)
        _seed_graph_nodes(
            conn,
            [("concept:x", "concept", {}, _zero_vec())],
        )
        _checkpoint_generation(conn, 1)

    linker = SemanticConceptLinker(
        source_subtype="publication",
        section_subtype="section",
        parent_attr="pub_id",
        min_similarity=0.5,
    )
    with caplog.at_level(
        logging.WARNING,
        logger="okg.substrate.library.enrichers.semantic_concept_linker",
    ):
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            result = linker.enrich(conn, generation_id=1)
    assert result.n_edges_emitted == 0
    assert any(
        "zero section nodes have embeddings" in rec.message
        for rec in caplog.records
    )


# ---------- enrich() happy path ----------


def test_enrich_emits_mentions_edges_for_above_threshold_matches(
    clean_runtime: str,
) -> None:
    """A publication with one section whose embedding is identical
    to a concept's embedding (cosine = 1.0) emits a mentions edge
    with confidence ≈ 1.0."""
    # Use a simple schema-dim embedding shape where one direction is 1.
    one_hot = _one_hot(0)  # cosine similarity to itself = 1.0

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _ensure_generation(conn, 1)
        _seed_graph_nodes(
            conn,
            [
                # 1 concept with the one-hot embedding.
                ("concept:x", "concept", {}, one_hot),
                # 1 publication (the source) — embedding optional.
                ("pub:1", "publication", {}, None),
                # 1 section attached to the publication via parent_attr,
                # also one-hot so cosine ≈ 1.
                ("sec:1", "section", {"pub_id": "pub:1"}, one_hot),
            ],
        )
        _checkpoint_generation(conn, 1)

    linker = SemanticConceptLinker(
        source_subtype="publication",
        section_subtype="section",
        parent_attr="pub_id",
        min_similarity=0.5,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        result = linker.enrich(conn, generation_id=1)

    assert result.n_edges_emitted == 1
    # Confirm the edge fact landed.
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        _ensure_generation(conn, 1)
        _ensure_generation(conn, 2)
        _ensure_generation(conn, 3)
        cur.execute(
            "SELECT src, dst, edge_type, provenance, confidence "
            "  FROM okg.edge_facts "
            " WHERE src = 'pub:1' AND dst = 'concept:x'"
        )
        row = cur.fetchone()
    assert row is not None
    src, dst, edge_type, provenance, confidence = row
    assert edge_type == "mentions"
    assert provenance == "derived_semantic"
    assert confidence > 0.99  # one-hot vs one-hot: cosine ≈ 1


def test_enrich_reads_concepts_and_sections_at_requested_generation(
    clean_runtime: str,
) -> None:
    """The linker must not use current rows when enriching an older pin."""
    one_hot = _one_hot(0)
    other_hot = _one_hot(1)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _ensure_generation(conn, 1)
        _ensure_generation(conn, 2)
        _ensure_generation(conn, 3)
        # Generation 1: concept:x carries the one-hot embedding, alongside
        # the publication and its one-hot section (cosine ≈ 1).
        _seed_graph_nodes(
            conn,
            [
                ("concept:x", "concept", {}, one_hot),
                ("pub:1", "publication", {}, None),
                ("sec:1", "section", {"pub_id": "pub:1"}, one_hot),
            ],
        )
        _checkpoint_generation(conn, 1)
        # Generation 3 (the current state): concept:x is rewritten with an
        # orthogonal embedding (cosine = 0). A linker reading the current
        # rows instead of the gen-1 pin would emit no edge.
        _seed_graph_nodes(
            conn,
            [("concept:x", "concept", {}, other_hot)],
        )
        _checkpoint_generation(conn, 3)

    linker = SemanticConceptLinker(
        source_subtype="publication",
        section_subtype="section",
        parent_attr="pub_id",
        min_similarity=0.5,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        result = linker.enrich(conn, generation_id=1)

    assert result.n_edges_emitted == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        _ensure_generation(conn, 1)
        cur.execute(
            "SELECT source_revision "
            "  FROM okg.edge_facts "
            " WHERE src = 'pub:1' AND dst = 'concept:x'"
        )
        row = cur.fetchone()

    assert row is not None
    assert row[0]["generation_id"] == 1


def test_enrich_skips_below_threshold_matches(
    clean_runtime: str,
) -> None:
    """A section orthogonal to the concept (cosine = 0) is below
    threshold and emits no edge."""
    one_hot = _one_hot(0)
    other_hot = _one_hot(1)  # cosine = 0

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _ensure_generation(conn, 1)
        _seed_graph_nodes(
            conn,
            [
                ("concept:x", "concept", {}, one_hot),
                ("pub:1", "publication", {}, None),
                # section orthogonal to the concept (cosine = 0).
                ("sec:1", "section", {"pub_id": "pub:1"}, other_hot),
            ],
        )
        _checkpoint_generation(conn, 1)

    linker = SemanticConceptLinker(
        source_subtype="publication",
        section_subtype="section",
        parent_attr="pub_id",
        min_similarity=0.5,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        result = linker.enrich(conn, generation_id=1)

    assert result.n_edges_emitted == 0
