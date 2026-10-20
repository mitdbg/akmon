"""Executed-test half of task.cards.record-identity.

Covers req.cards.record-identity from
pact/changes/mcp-candidate-cards-and-coverage/pact.yaml: a lexical or semantic
candidate card names the SOURCE RECORD it came from, and says so explicitly
when the producer attributed none.

The explicit-null half is the part worth stating. `graph_nodes.source_record_id`
and `source_revision` are `JSONB NOT NULL DEFAULT '{}'`, so an unattributed row
holds the EMPTY OBJECT rather than SQL NULL. A card that echoed `{}` would put a
value-shaped thing in front of an agent that could mistake it for an identity it
can cite or diff, so the empty container is normalized to an explicit JSON null
and the key stays present either way.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp import open_session, search
from okg.substrate.mcp.provenance import FENCE_CLOSE, FENCE_OPEN
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp.search_fixture import (
    make_search_deployment,
    node,
    publish_nodes,
)


pytestmark = pytest.mark.postgres


CHUNK = SubtypeSpec("analysis_chunk", metadata={"searchable_text_fields": ["text"]})
QUERY = "beryllium calorimeter linearity"


def _fenced(value: str) -> str:
    """The fixture producer is unprofiled, so its record id is fenced (#2689)."""
    opener = FENCE_OPEN.format(source="system:analyst_search_fixture")
    return f"{opener}\n{value}\n{FENCE_CLOSE}"


def _unattributed(node_id: str, text: str) -> NodeFact:
    """A node whose producer supplied no record identity at all.

    `search_fixture.node` always stamps one, which is the normal case; this is
    the case the explicit-null rule exists for.
    """
    return NodeFact(
        node_id=node_id,
        subtype=CHUNK.name,
        attrs={"text": text},
        source_record_id={},
        source_revision={},
    )


def test_lexical_card_carries_the_source_record_by_default(
    clean_runtime: str,
    tmp_path,
) -> None:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=[CHUNK],
        nodes=[node("chunk:attributed", CHUNK.name, f"The {QUERY} was measured.")],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        page = search(conn, session, QUERY, method="lexical")

    row = page["results"][0]
    assert row["node_id"] == "chunk:attributed"
    # Default projection: no `select` was passed.
    assert row["source_record_id"] == {"id": _fenced("chunk:attributed")}
    assert row["source_revision"] == {"v": 1}


def test_an_unattributed_row_reports_explicit_nulls_not_empty_objects(
    clean_runtime: str,
    tmp_path,
) -> None:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=[CHUNK],
        nodes=[_unattributed("chunk:bare", f"A bare {QUERY} record.")],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        page = search(conn, session, QUERY, method="lexical")

    row = page["results"][0]
    assert row["node_id"] == "chunk:bare"
    # Present-and-null, never absent and never `{}`: an absent key reads as
    # "this tool does not report source records" and `{}` reads as a value.
    assert "source_record_id" in row
    assert "source_revision" in row
    assert row["source_record_id"] is None
    assert row["source_revision"] is None


def test_select_can_name_the_record_fields_and_can_drop_them(
    clean_runtime: str,
    tmp_path,
) -> None:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=[CHUNK],
        nodes=[node("chunk:selectable", CHUNK.name, f"Selecting the {QUERY}.")],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        named = search(
            conn,
            session,
            QUERY,
            method="lexical",
            select=["node_id", "source_record_id", "source_revision"],
        )
        narrowed = search(
            conn, session, QUERY, method="lexical", select=["node_id"],
        )

    assert named["results"][0]["source_record_id"] == {"id": _fenced("chunk:selectable")}
    assert named["results"][0]["source_revision"] == {"v": 1}
    # A caller who narrowed `select` to keep the response small must actually
    # get a small response: the record fields are default-projected, not part
    # of the identity floor a caller cannot project away.
    assert "source_record_id" not in narrowed["results"][0]
    assert "source_revision" not in narrowed["results"][0]


def test_the_record_fields_are_pinned_to_the_session_generation(
    clean_runtime: str,
    tmp_path,
) -> None:
    """A later publish does not change what a pinned card says."""
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    _cv, first_generation = publish_nodes(
        clean_runtime,
        deployment,
        subtypes=[CHUNK],
        nodes=[
            NodeFact(
                node_id="chunk:revised",
                subtype=CHUNK.name,
                attrs={"text": f"Revision one of the {QUERY}."},
                source_record_id={"id": "rec-1"},
                source_revision={"v": 1},
            )
        ],
    )
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=[CHUNK],
        nodes=[
            NodeFact(
                node_id="chunk:revised",
                subtype=CHUNK.name,
                attrs={"text": f"Revision two of the {QUERY}."},
                source_record_id={"id": "rec-1"},
                source_revision={"v": 2},
            )
        ],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        pinned = open_session(
            conn,
            deployment_name=deployment,
            requested_generation=first_generation,
        )
        latest = open_session(conn, deployment_name=deployment)
        pinned_page = search(conn, pinned, QUERY, method="lexical")
        latest_page = search(conn, latest, QUERY, method="lexical")

    assert pinned_page["results"][0]["source_revision"] == {"v": 1}
    assert latest_page["results"][0]["source_revision"] == {"v": 2}
