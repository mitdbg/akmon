"""Regression: a publish that carries only an overlay change leaves no derived
state behind (PACT lane-s-storage-redesign-s2, requirement
``overlay-only-publish``; parent design section 1.9).

The defect this guards against -- ``okg.attr_index`` falling behind an
overlay-only publish, so the next publish touching that target was refused --
was fixed by okg#3112 step 4b, which removed ``attr_index``. On dev after 4b
this test has no failing case; it existed only on a commit before 4b
(inferred from the 2026-10-05 Krama-copy run). It is kept so a derived copy
that again tracks only graph changes is caught.
"""
from __future__ import annotations

from typing import Any

import psycopg
import pytest

from okg.substrate.attributes import (
    materialize_effective_attributes_for_generation,
    record_attribute_observation,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp import server as server_mod
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import (
    SubtypeSpec,
    bind_test_catalog_writer,
    seed_catalog,
)

pytestmark = pytest.mark.postgres

TARGET = "doc:overlay-only:target"
OTHER = "doc:overlay-only:other"


def _apply(dsn: str, facts: list[NodeFact]) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            facts, source="default", strict_endpoints=True,
        )


def _cycle(dsn: str) -> dict[str, Any]:
    out = Publisher(
        dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()
    assert out["status"] == "published", out
    return out


def _filter_ids(dsn: str, generation: str, value: str) -> list[str]:
    mcp = server_mod.build_server(dsn=dsn, deployment_name="default")
    try:
        page = mcp._tool_manager._tools["filter"].fn(
            input={"kind": "nodes", "subtype": "doc"},
            predicate={"op": "eq", "field": "attrs.path", "value": value},
            generation_id=generation, limit=10,
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    return [row["node_id"] for row in page["items"]]


def test_an_overlay_only_publish_then_a_graph_change_publishes_and_reads_the_overlay(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=[SubtypeSpec(
            name="doc", metadata={"filterable_attrs": {"path": "text"}},
        )])
    _apply(dsn, [
        NodeFact(TARGET, "doc", {"path": "graph.py", "rank": 1}, {"id": TARGET}, {"v": 1}),
        NodeFact(OTHER, "doc", {"path": "other.py", "rank": 2}, {"id": OTHER}, {"v": 1}),
    ])
    g0 = str(_cycle(dsn)["generation_id"])

    # The standard post-publish path: an accepted overlay on the declared
    # attribute, materialized into the published generation.
    with psycopg.connect(dsn, autocommit=True) as conn:
        record_attribute_observation(
            conn, target_kind="node", target_id=TARGET, generation_id=g0,
            attr_key="path", value="overlaid.py", producer="test:overlay-only",
        )
        materialize_effective_attributes_for_generation(conn, generation_id=g0)

    # A publish that carries only that overlay change (no graph write). The
    # publisher may reuse g0; either way it must publish.
    g1 = str(_cycle(dsn)["generation_id"])

    # A graph change to the same target must publish, not be refused.
    _apply(dsn, [
        NodeFact(TARGET, "doc", {"path": "graph.py", "rank": 3}, {"id": TARGET}, {"v": 2}),
    ])
    g2_out = _cycle(dsn)
    assert not g2_out.get("noop"), g2_out
    g2 = str(g2_out["generation_id"])
    assert g2 not in (g0, g1)

    for generation in (g0, g1, g2):
        assert _filter_ids(dsn, generation, "overlaid.py") == [TARGET], generation
        assert _filter_ids(dsn, generation, "graph.py") == [], generation
