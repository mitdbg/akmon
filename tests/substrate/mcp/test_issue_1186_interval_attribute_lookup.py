"""Public MCP regression coverage for GitHub issue #1186.

The fixture deliberately publishes two generations; each generation-pinned
read returns its own generation's value. The proof goes through an in-memory
MCP ``ClientSession``. Since PACT lane-s-storage-cutover every read takes the
graph rows at the pin (no interval or snapshot copy is read), so the old
source-resolver refusals and the snapshot mode are gone with that path.
"""
from __future__ import annotations

from tests.substrate.mcp._result_payload import (
    json_client_sessions,
    payload as _result_payload,
    result_text as _result_text,
)


import anyio
import psycopg
import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp.server import build_server
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres


@pytest.fixture(autouse=True)
def _json_result_format(monkeypatch: pytest.MonkeyPatch) -> None:
    """These tests assert on the dictionary, so every client call asks for
    the JSON result format (issue #3059: the default is one text block)."""
    json_client_sessions(monkeypatch)

NODE = "document_chunk:issue-1186:caption"
FILE = "source_file:issue-1186:note"
EDGE = "edge:issue-1186:caption-links-note"


@pytest.fixture(autouse=True)
def _mcp_role_can_log_in(clean_runtime: str, okg_mcp_can_log_in: str) -> None:
    """``okg_mcp`` can log in.

    Set once per session under the cluster role lock and never reverted
    per test; see ``okg_mcp_login_enabled`` in tests/substrate/conftest.py.
    """


def _publish_two_generations(dsn: str) -> tuple[str, str]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["document_chunk", "source_file"],
            narrowings=[("document_chunk", "links", "source_file")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    node_id=NODE,
                    subtype="document_chunk",
                    attrs={
                        "chunk_kind": "image_caption",
                        "version": 1,
                        "text": "caption alpha figure",
                    },
                    source_record_id={"id": NODE},
                    source_revision={"v": 1},
                ),
                NodeFact(
                    node_id=FILE,
                    subtype="source_file",
                    attrs={"path": "notes/issue-1186.md", "version": 1},
                    source_record_id={"id": FILE},
                    source_revision={"v": 1},
                ),
                EdgeFact(
                    src=NODE,
                    dst=FILE,
                    edge_type="links",
                    attrs={"relation": "caption-v1"},
                    source_record_id={"id": EDGE},
                    source_revision={"v": 1},
                ),
            ],
            source="fixture",
            strict_endpoints=True,
        )
    first = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert first["status"] == "published", first

    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    node_id=NODE,
                    subtype="document_chunk",
                    attrs={
                        "chunk_kind": "image_caption",
                        "version": 2,
                        "text": "caption alpha figure",
                    },
                    source_record_id={"id": NODE},
                    source_revision={"v": 2},
                ),
                NodeFact(
                    node_id=FILE,
                    subtype="source_file",
                    attrs={"path": "notes/issue-1186.md", "version": 2},
                    source_record_id={"id": FILE},
                    source_revision={"v": 2},
                ),
                EdgeFact(
                    src=NODE,
                    dst=FILE,
                    edge_type="links",
                    attrs={"relation": "caption-v2"},
                    source_record_id={"id": EDGE},
                    source_revision={"v": 2},
                ),
            ],
            source="fixture",
            strict_endpoints=True,
        )
    second = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert second["status"] == "published", second
    return str(first["generation_id"]), str(second["generation_id"])


def _close(server) -> None:
    server._okg_pool.close()
    if server._okg_write_pool:
        server._okg_write_pool.close()


def test_issue_1186_public_client_session_pinned_reads(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Generation-pinned filters, the attrs lookup and the edge filters each
    return their own generation's value."""
    first, second = _publish_two_generations(clean_runtime)
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.generation_ordinals "
            "WHERE checkpoint_id = ANY(%s)",
            ([first, second],),
        )
        assert cur.fetchone()[0] == 2
    server = build_server(dsn=clean_runtime, deployment_name="default")

    async def drive() -> None:
        async with create_connected_server_and_client_session(server) as session:
            old_filter = await session.call_tool(
                "filter",
                {
                    "input": {"kind": "nodes", "subtype": "document_chunk"},
                    "predicate": {
                        "op": "eq",
                        "field": "attrs.version",
                        "value": 1,
                    },
                    "select": ["node_id", "subtype", "attrs"],
                    "generation_id": first,
                },
            )
            new_filter = await session.call_tool(
                "filter",
                {
                    "input": {"kind": "nodes", "subtype": "document_chunk"},
                    "predicate": {
                        "op": "eq",
                        "field": "attrs.version",
                        "value": 2,
                    },
                    "select": ["node_id", "subtype", "attrs"],
                    "generation_id": second,
                },
            )
            for result, generation, expected in (
                (old_filter, first, 1),
                (new_filter, second, 2),
            ):
                assert result.isError is False, result
                payload = _result_payload(result)
                assert payload["pinned_generation_id"] == generation
                assert len(payload["items"]) == 1, payload
                assert payload["items"][0]["node_id"] == NODE
                assert payload["items"][0]["attrs"]["version"] == expected

            attrs_lookup = await session.call_tool(
                "search",
                {
                    "method": "attrs",
                    "attr_filters": {"version": 1},
                    "subtype_filter": "document_chunk",
                    "generation_id": first,
                },
            )
            assert attrs_lookup.isError is False, _result_text(attrs_lookup)
            attrs_payload = _result_payload(attrs_lookup)
            assert attrs_payload["pinned_generation_id"] == first
            assert [row["node_id"] for row in attrs_payload["results"]] == [NODE]
            assert attrs_payload["results"][0]["attrs"]["version"] == 1

            edge = await session.call_tool(
                "filter",
                {
                    "input": {"kind": "edges", "edge_type": "links"},
                    "predicate": {
                        "op": "eq",
                        "field": "attrs.relation",
                        "value": "caption-v1",
                    },
                    "select": ["edge_id", "src", "dst", "edge_type", "attrs"],
                    "generation_id": first,
                },
            )
            assert edge.isError is False, _result_text(edge)
            edge_payload = _result_payload(edge)
            assert edge_payload["pinned_generation_id"] == first
            assert edge_payload["items"], edge_payload
            assert {
                "edge_id", "src", "dst", "edge_type", "attrs",
            } <= set(edge_payload["items"][0])
            assert edge_payload["items"][0]["edge_id"]
            assert "caption-v1" in edge_payload["items"][0]["attrs"]["relation"]

            edge_new = await session.call_tool(
                "filter",
                {
                    "input": {"kind": "edges", "edge_type": "links"},
                    "predicate": {
                        "op": "eq",
                        "field": "attrs.relation",
                        "value": "caption-v2",
                    },
                    "select": ["edge_id", "src", "dst", "edge_type", "attrs"],
                    "generation_id": second,
                },
            )
            assert edge_new.isError is False, _result_text(edge_new)
            edge_new_payload = _result_payload(edge_new)
            assert edge_new_payload["pinned_generation_id"] == second
            assert edge_new_payload["items"], edge_new_payload
            assert "caption-v2" in edge_new_payload["items"][0]["attrs"]["relation"]

    try:
        anyio.run(drive)
    finally:
        _close(server)
