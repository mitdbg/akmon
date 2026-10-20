"""Hermetic blocking receipt for the selected passage aggregation."""

from __future__ import annotations

import psycopg
import pytest

from okg.substrate.mcp import open_session, search
from okg.substrate.mcp.passage_rollup import PASSAGE_ROLLUP_AGGREGATION
from tests.substrate.mcp.erag_rollup_fixture import (
    EXPECTED_CHUNK_COUNTS,
    NARROWINGS,
    SUBTYPES,
    TERM,
    build_facts,
)
from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes


pytestmark = pytest.mark.postgres


def test_max_aggregation_replays_on_the_hermetic_passage_fixture(
    clean_runtime: str,
    tmp_path,
) -> None:
    """A real published fixture makes the measured ``max`` rule observable."""
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    facts = build_facts()
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=SUBTYPES,
        narrowings=NARROWINGS,
        nodes=[fact for fact in facts if hasattr(fact, "node_id")],
        edges=[fact for fact in facts if not hasattr(fact, "node_id")],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        page = search(
            conn,
            session,
            TERM,
            method="lexical", completeness="exact",
            filter={"subtype": "doc"},
            rank_by="passages",
            limit=10,
        )

    assert PASSAGE_ROLLUP_AGGREGATION == "max"
    assert page["rank_by_aggregation"] == "max"
    assert page["total_count"] == len(EXPECTED_CHUNK_COUNTS)
    assert page["results"][0]["node_id"] == "doc:sharp"
    assert page["rollup_contributing_chunks"]["doc:sharp"] == 2
