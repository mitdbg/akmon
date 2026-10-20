"""Batch-scoped caching in the entity-resolution stage.

`resolve()` used to rebuild three things per entity: the Chronos checkout, the
active catalog version, and the subtype's live normalized-name index -- the
last being a full scan of every live node of that subtype. For N entities
against M live nodes that is N*M rows read. These tests pin the two halves of
the fix: the scan happens once per batch, and `begin_batch()` really does drop
the cache so a later cycle sees newly landed nodes.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.entity_resolution import EntityResolutionStage
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact

pytestmark = pytest.mark.postgres


def _config() -> dict:
    return {
        "deployment_name": "entity-resolution-test",
        "branch_name": "default",
        "algorithm_version": "unicode-escape-v1",
        "policies": {
            "concept": {
                "name_field": "name",
                "id_prefix": "concept",
                "identity_fields": ["concept_id"],
                "alias_first": True,
                "live_exact": True,
                "post_filter": {"deny_exact": []},
            }
        },
    }


def _seed_catalog(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions "
            "(status, applied_at, deployment_name) "
            "VALUES ('applied', now(), 'entity-resolution-test') "
            "RETURNING catalog_version_id"
        )
        return int(cur.fetchone()[0])


def _seed_nodes(conn: psycopg.Connection, rows: list[tuple[str, str]]) -> None:
    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts(
        [
            NodeFact(
                node_id=node_id,
                subtype="concept",
                attrs={"name": name},
                source_record_id={"id": node_id},
                source_revision={"v": 1},
            )
            for node_id, name in rows
        ],
        branch="default",
        source="fixture",
    )


def test_live_scan_runs_once_per_batch_not_once_per_entity(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)
        _seed_nodes(conn, [(f"concept:seed-{n}", f"Seeded {n}") for n in range(5)])

        stage = EntityResolutionStage(_config())
        stage.begin_batch()

        # Force the batch checkout, then spy on it. The stage caches the
        # checkout, so every resolve below goes through this wrapper. Counting
        # `graph.query` calls filtered by subtype isolates the full scan from
        # the per-node `node_id =` lookups, which also read graph_nodes.
        graph = stage._batch_graph(conn)
        scans = []
        real_query = graph.query

        def counting_query(sql, params=None, *a, **k):
            if "subtype" in str(sql) and "node_id =" not in str(sql):
                scans.append(str(sql))
            return real_query(sql, params, *a, **k)

        graph.query = counting_query

        for i in range(8):
            stage.resolve(
                conn,
                proposed_node_id=f"concept:novel-{i}",
                subtype="concept",
                attrs={"name": f"Novel Entity {i}"},
            )

    # Eight novel identities, one scan. Before batching this was eight.
    assert len(scans) == 1, scans


def test_begin_batch_drops_the_live_index_so_later_cycles_see_new_nodes(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)
        stage = EntityResolutionStage(_config())

        # Cycle 1 populates the index while "Late Arrival" does not exist.
        stage.begin_batch()
        stage.resolve(
            conn,
            proposed_node_id="concept:first",
            subtype="concept",
            attrs={"name": "Something Else"},
        )

        # A different writer lands the node between cycles.
        _seed_nodes(conn, [("concept:late", "Late Arrival")])

        # Cycle 2 must see it and adopt it rather than minting a duplicate.
        stage.begin_batch()
        decision = stage.resolve(
            conn,
            proposed_node_id="concept:would-duplicate",
            subtype="concept",
            attrs={"name": "late arrival"},
        )

    assert decision.canonical_node_id == "concept:late"
    assert decision.method == "live_normalized_exact"


def test_batch_cache_still_adopts_a_node_minted_earlier_in_the_same_batch(
    clean_runtime: str,
) -> None:
    """The identity claim, not the live index, covers same-batch repeats.

    The live index is built once and does not include nodes this batch mints,
    so the claim lookup that runs before it is what stops a batch from minting
    two canonicals for one name.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)
        stage = EntityResolutionStage(_config())
        stage.begin_batch()

        first = stage.resolve(
            conn,
            proposed_node_id="concept:a",
            subtype="concept",
            attrs={"name": "Repeated Name"},
        )
        second = stage.resolve(
            conn,
            proposed_node_id="concept:b",
            subtype="concept",
            attrs={"name": "repeated   name"},
        )

    assert first.canonical_node_id == second.canonical_node_id
