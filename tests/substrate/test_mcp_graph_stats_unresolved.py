"""Per `refine-doc-corpus-extraction` (2026-04-26) W4:
`graph_stats.unresolved_mentions` reports mention-resolution
rate at the pin.

Seed 5 entity_mentions; resolve 2 via canonicalization edges. Assert
graph_stats returns total=5 / resolved=2 / rate_resolved=0.4
plus the per-entity-type breakdown ordered by total desc.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any

import psycopg
import pytest


pytestmark = pytest.mark.postgres


@pytest.fixture(autouse=True)
def _json_result_format(monkeypatch: pytest.MonkeyPatch):
    """These tests assert on the result dictionary, so every call asks for
    the JSON result format (issue #3059: the default is one text block).
    Client sessions send the request _meta key; in-process
    FastMCP.call_tool calls run inside result_format_scope."""
    from okg.substrate.mcp.result_text import result_format_scope
    from tests.substrate.mcp._result_payload import json_client_sessions

    json_client_sessions(monkeypatch)
    with result_format_scope("json"):
        yield


def _tool_payload(res: Any) -> dict[str, Any]:
    """The JSON payload of one ``call_tool`` result, refusing an error result.

    Under mcp 2 ``call_tool`` returns a ``CallToolResult``. A refusal carries
    its envelope in ``structuredContent`` with ``isError`` set and NO text
    content, so joining the text blocks gave ``""`` and every refusal surfaced
    as a bare ``JSONDecodeError`` instead of the refusal that caused it.
    """
    if getattr(res, "isError", False):
        raise AssertionError(f"tool call refused: {res.content!r}")
    structured = getattr(res, "structuredContent", None)
    if isinstance(structured, dict):
        return structured
    contents = res[0] if isinstance(res, tuple) else getattr(res, "content", res)
    return json.loads("; ".join(c.text for c in contents if hasattr(c, "text")))


def _seed(conn: psycopg.Connection) -> str:
    """Seed catalog + 5 mentions + 2 canonicalization edges + the
    canonical target nodes those edges point at. Returns
    generation_id of the published graph."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO okg.subtypes "
            "  (name, catalog_version_id) VALUES "
            "  ('entity_mention', %s), ('document_chunk', %s), "
            "  ('open_spec_change', %s)",
            (cv, cv, cv),
        )
        cur.execute(
            """
            INSERT INTO okg.subtype_ancestors
              (subtype_name, ancestor_name, depth, catalog_version_id)
            VALUES
              ('entity_mention', 'entity_mention', 0, %s),
              ('document_chunk', 'document_chunk', 0, %s),
              ('open_spec_change', 'open_spec_change', 0, %s)
            """,
            (cv, cv, cv),
        )
        # Narrowings: `mentions` is canonical; `references` is accepted
        # for the transition from the earlier declarative-linker edge.
        cur.execute(
            "INSERT INTO okg.narrowings "
            "  (parent_subtype, edge_type, child_subtype, "
            "   catalog_version_id) "
            "VALUES "
            "('entity_mention','mentions','open_spec_change',%s), "
            "('entity_mention','references','open_spec_change',%s)",
            (cv, cv),
        )
    store = ChronosGraphStore.from_connection(conn)
    facts = [
        *[
            NodeFact(
                f"mention:slug-{i}",
                "entity_mention",
                {"entity_type": "change_slug"},
                {"id": f"mention:slug-{i}"},
                {"v": 1},
            )
            for i in range(3)
        ],
        *[
            NodeFact(
                f"mention:arxiv-{i}",
                "entity_mention",
                {"entity_type": "arxiv_id"},
                {"id": f"mention:arxiv-{i}"},
                {"v": 1},
            )
            for i in range(2)
        ],
        *[
            NodeFact(
                f"change:c-{i}",
                "open_spec_change",
                {},
                {"id": f"change:c-{i}"},
                {"v": 1},
            )
            for i in range(2)
        ],
        EdgeFact(
            "mention:slug-0",
            "change:c-0",
            "mentions",
            source_record_id={"id": "edge:0"},
            source_revision={"v": 1},
        ),
        EdgeFact(
            "mention:slug-1",
            "change:c-1",
            "references",
            source_record_id={"id": "edge:1"},
            source_revision={"v": 1},
        ),
    ]
    store.apply_facts(facts)
    return store.checkpoint_generation(
        deployment_name="test",
        branch="default",
        catalog_version_id=cv,
        metadata={"correlation_id": "t1"},
    )


def test_graph_stats_unresolved_mentions(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)

    from okg.substrate.mcp.server import build_server

    mcp = build_server(
        dsn=clean_runtime,
        deployment_name="test",
    )

    async def _drive():
        res = await mcp.call_tool(
            "inspect",
            {"target": {"kind": "graph"}},
        )
        return _tool_payload(res)

    payload = asyncio.run(_drive())
    block = payload["unresolved_mentions"]
    assert block["total"] == 5
    assert block["resolved"] == 2
    assert abs(block["rate_resolved"] - 0.4) < 1e-9
    by_type = block["by_entity_type"]
    # change_slug: 3 total / 2 resolved
    assert by_type["change_slug"]["total"] == 3
    assert by_type["change_slug"]["resolved"] == 2
    # arxiv_id: 2 total / 0 resolved
    assert by_type["arxiv_id"]["total"] == 2
    assert by_type["arxiv_id"]["resolved"] == 0


def test_existing_graph_stats_fields_preserved(
    clean_runtime: str,
) -> None:
    """Additive contract: every existing field stays present."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)

    from okg.substrate.mcp.server import build_server

    mcp = build_server(
        dsn=clean_runtime,
        deployment_name="test",
    )

    async def _drive():
        res = await mcp.call_tool(
            "inspect",
            {"target": {"kind": "graph"}},
        )
        return _tool_payload(res)

    payload = asyncio.run(_drive())
    for required in (
        "deployment_name", "generation_id", "catalog_version_id",
        "node_count", "edge_count", "subtype_count",
        "edge_type_count", "embed_lag",
    ):
        assert required in payload, required


def test_inspect_graph_sql_summary_matches_per_row(clean_runtime: str) -> None:
    """The SQL-aggregate summary must equal the per-row summary field-for-field.

    `inspect(graph)` now computes counts/embed_lag/unresolved_mentions as SQL
    aggregates instead of materializing every node/edge and looping in Python.
    On a Nomos-off deployment both paths see the whole graph, so
    `_graph_summary_sql` (the new path) must reproduce `_graph_summary_per_row`
    (the original loop) exactly.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)

    from okg.substrate.mcp.session import open_session
    from okg.substrate.mcp.tools import (
        INSPECT_GRAPH_BLOCKS,
        _chronos_checkout,
        _graph_summary_per_row,
        _graph_summary_sql,
    )

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="test")
        graph = _chronos_checkout(conn, session)
        # All four blocks: the SQL and per-row summaries must agree on the FULL
        # payload, not just the subset a narrowed `select` would compute.
        sql_summary = _graph_summary_sql(graph, INSPECT_GRAPH_BLOCKS)
        per_row_summary = _graph_summary_per_row(conn, session)

    assert sql_summary == per_row_summary
    # Sanity: the seeded graph is the mention fixture (5 mentions, 2 edges).
    assert sql_summary["unresolved_mentions"]["total"] == 5
    assert sql_summary["unresolved_mentions"]["resolved"] == 2


def test_inspect_graph_reports_visibility_and_truncated(
    clean_runtime: str,
) -> None:
    """Additive fields: `visibility` + `truncated` describe how counts were
    derived. On a Nomos-off deployment the whole graph is aggregated, so
    visibility is 'unfiltered' and nothing is truncated."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)

    from okg.substrate.mcp.session import open_session
    from okg.substrate.mcp.tools import inspect_graph

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="test")
        payload = inspect_graph(conn, session)

    assert payload["visibility"] == "unfiltered"
    assert payload["truncated"] is False
    assert payload["node_count"] == 7  # 5 mentions + 2 change targets
    assert payload["edge_count"] == 2
