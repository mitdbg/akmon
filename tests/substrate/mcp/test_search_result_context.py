from __future__ import annotations

import psycopg
import pytest

from okg.substrate.mcp import open_session, search
from tests.substrate.mcp.search_fixture import (
    make_search_deployment,
    node,
    publish_nodes,
)


pytestmark = pytest.mark.postgres


def test_result_context_is_bounded_deterministic_and_generation_pinned(
    clean_runtime: str,
    tmp_path,
) -> None:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    text = "prefix " * 100 + "PuppiMET requirement selection" + " suffix" * 100
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=["analysis_chunk"],
        nodes=[node("chunk:puppimet", "analysis_chunk", text)],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        first = search(
            conn,
            session,
            "PuppiMET requirement selection",
            method="lexical",
        )
        second = search(
            conn,
            session,
            "PuppiMET requirement selection",
            method="lexical",
        )
    result = first["results"][0]
    assert result["canonical_node_id"] == "chunk:puppimet"
    assert result["method"] == "lexical"
    assert isinstance(result["score"], float)
    assert "PuppiMET requirement selection" in result["excerpt"]
    # The budget counts source text only (issue #2670): this fixture's
    # producer is untrusted, so the markers ride on top of the 280 chars.
    assert result["provenance"]["trust"] == "untrusted"
    assert len("\n".join(result["excerpt"].split("\n")[1:-1])) <= 280
    assert first["generation_id"] == first["pinned_generation_id"]
    assert first["results"] == second["results"]


def test_select_can_still_suppress_match_context(
    clean_runtime: str,
    tmp_path,
) -> None:
    """Match context is a default, not a floor; evidence is a floor.

    A caller narrowing `select` to keep responses small must actually get a
    small response; excerpt/method/match_kind are rich enough that pinning them
    into the always-projected set would silently inflate every page.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    text = "prefix " * 100 + "PuppiMET requirement selection" + " suffix" * 100
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=["analysis_chunk"],
        nodes=[node("chunk:puppimet", "analysis_chunk", text)],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        narrowed = search(
            conn,
            session,
            "PuppiMET requirement selection",
            method="lexical",
            select=["node_id"],
        )
    row = narrowed["results"][0]
    assert row["node_id"] == "chunk:puppimet"
    assert "excerpt" not in row
    assert "match_kind" not in row
    assert "canonical_node_id" not in row
    assert row["evidence_kind"] == "unknown"
    assert "evidence_kind_provenance" in row
