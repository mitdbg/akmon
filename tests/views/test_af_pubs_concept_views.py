"""Regression tests for af_pubs concept-derived view corpora."""
from __future__ import annotations

import psycopg
import pytest

pytest.importorskip("views.concept_corpus")
from views.concept_corpus import bad_weather_build_corpus

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact

pytestmark = pytest.mark.postgres


def _ensure_narrowing(
    cur: psycopg.Cursor,
    parent_subtype: str,
    edge_type: str,
    child_subtype: str,
) -> None:
    cur.execute(
        """
        INSERT INTO okg.narrowings
          (parent_subtype, edge_type, child_subtype, catalog_version_id)
        VALUES (
          %s, %s, %s,
          COALESCE((
            SELECT max(catalog_version_id)
              FROM okg.catalog_versions
             WHERE status = 'applied'
          ), 1)
        )
        ON CONFLICT DO NOTHING
        """,
        (parent_subtype, edge_type, child_subtype),
    )


def test_concept_corpus_reads_mentions_at_materialization_generation(
    clean_runtime: str,
) -> None:
    # Chronos cutover: the live graph (concept + publication nodes and the
    # `mentions` edges) lives in okg.graph_nodes/graph_edges, read by
    # bad_weather_build_corpus via checkout_generation(gen). The legacy
    # bitemporal window (valid_from_gen=1, valid_to_gen=2 for `pinned`;
    # valid_from_gen=3 for `current`) is expressed as Chronos checkpoints:
    # generation 1 captures the concept + `pinned` + the pinned->concept
    # edge; the gen-3 transition retracts `pinned`/its edge and adds
    # `current`/its edge before checkpointing generation 3. A read pinned
    # to generation 1 therefore sees `pinned` but not `current`.
    concept_id = "concept:flying_in_bad_weather"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            _ensure_narrowing(cur, "publication", "mentions", "concept")
            for gen in (1, 2, 3):
                cur.execute(
                    "INSERT INTO okg.graph_generations "
                    "  (generation_id, deployment_name, status, "
                    "   catalog_version_id, published_at, correlation_id) "
                    "VALUES (%s, 'view-test', 'published', 1, now(), %s) "
                    "ON CONFLICT (generation_id) DO NOTHING",
                    (gen, f"view-test:{gen}"),
                )

        ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)

        # Generation-1 state: concept + pinned publication + their edge.
        store.apply_facts(
            [
                NodeFact(
                    node_id=concept_id,
                    subtype="concept",
                    attrs={},
                    source_record_id={"id": concept_id},
                    source_revision={"v": 1},
                ),
                NodeFact(
                    node_id="publication:pinned",
                    subtype="publication",
                    attrs={
                        "pub_number": "PINNED",
                        "title": "Pinned publication",
                        "body_text": "pinned body",
                    },
                    source_record_id={"id": "publication:pinned"},
                    source_revision={"v": 1},
                ),
                EdgeFact(
                    src="publication:pinned",
                    dst=concept_id,
                    edge_type="mentions",
                    attrs={"evidence": {"similarity": 0.91}},
                    source_record_id={"id": "publication:pinned->concept"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
        store.checkpoint_generation(1, deployment_name="view-test")

        # Generation-3 transition: retract pinned + its edge, add current.
        store.apply_facts(
            [
                EdgeFact(
                    src="publication:pinned",
                    dst=concept_id,
                    edge_type="mentions",
                    source_record_id={"id": "publication:pinned->concept"},
                    source_revision={"v": 2},
                    op="R",
                ),
                NodeFact(
                    node_id="publication:pinned",
                    subtype="publication",
                    attrs={},
                    source_record_id={"id": "publication:pinned"},
                    source_revision={"v": 2},
                    op="R",
                ),
                NodeFact(
                    node_id="publication:current",
                    subtype="publication",
                    attrs={
                        "pub_number": "CURRENT",
                        "title": "Current publication",
                        "body_text": "current body",
                    },
                    source_record_id={"id": "publication:current"},
                    source_revision={"v": 1},
                ),
                EdgeFact(
                    src="publication:current",
                    dst=concept_id,
                    edge_type="mentions",
                    attrs={"evidence": {"similarity": 0.99}},
                    source_record_id={"id": "publication:current->concept"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
        store.checkpoint_generation(3, deployment_name="view-test")

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        corpus = bad_weather_build_corpus(conn, generation_id=1)

    assert "pub:publication:pinned:concept_corpus" in corpus
    assert "pub:publication:current:concept_corpus" not in corpus
