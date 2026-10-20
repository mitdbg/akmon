"""Public MCP regression coverage for GitHub issue #1186.

The fixture deliberately publishes two generations.  The default read mode
must stab the interval table and join its ``attrs_hash`` to the payload store;
the alternate snapshot mode is exercised only when its real snapshot exists.
The main two-generation proof goes through an in-memory MCP ``ClientSession``;
the missing-relation refusals are also pinned at the source-resolver seam so
the test can model absent relations without mutating the shared test database.
"""
from __future__ import annotations

from tests.substrate.mcp._result_payload import (
    json_client_sessions,
    payload as _result_payload,
    result_text as _result_text,
)


import anyio
import psycopg
import psycopg.sql
import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.errors import SubstrateError
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


class _MissingSnapshotCursor:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, _query, _params=None):
        return self

    def fetchone(self):
        return (None,)


class _MissingSnapshotConnection:
    def cursor(self):
        return _MissingSnapshotCursor()


class _IncompletePayloadCursor(_MissingSnapshotCursor):
    def execute(self, query, _params=None):
        self.query = " ".join(str(query).split())
        return self

    def fetchone(self):
        query = getattr(self, "query", "")
        row: tuple[object, object, object] = (None, None, None)
        if "to_regclass('okg.attribute_effective_view_intervals')" in query:
            row = ("okg.attribute_effective_view_intervals", None, None)
        elif "to_regclass('okg.v_generation_interval_stab')" in query:
            row = ("okg.v_generation_interval_stab", None, None)
        elif "max(stab_ordinal)" in query:
            # The completeness cache's branch-head read: the pinned ordinal
            # is the head, so the check runs (and refuses) as before.
            row = (7, None, None)
        elif "FROM okg.v_generation_interval_stab" in query:
            row = ("branch-1186", 7, None)
        elif "to_regclass('_chronos_branch_interval_checkpoints')" in query:
            row = (None, None, None)
        elif "AS interval_payload_rows" in query:
            row = (1, 1, 0)
        return row


class _IncompletePayloadConnection:
    # The completeness cache keys on the connection's database identity.
    info = type("_Info", (), {"host": "fake", "port": 0, "dbname": "issue1186"})()

    def cursor(self):
        return _IncompletePayloadCursor()


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


def test_issue_1186_public_client_session_interval_and_snapshot_reads(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first, second = _publish_two_generations(clean_runtime)
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM okg.attribute_effective_view_intervals iv
              LEFT JOIN okg.attribute_versions av
                USING (target_kind, target_id, attrs_hash)
            """
        )
        interval_rows = cur.fetchone()[0]
        assert interval_rows > 0
        cur.execute(
            """
            SELECT count(*), count(av.attrs_hash)
              FROM okg.attribute_effective_view_intervals iv
              LEFT JOIN okg.attribute_versions av
                USING (target_kind, target_id, attrs_hash)
            """
        )
        total_rows, payload_rows = cur.fetchone()
        assert total_rows == interval_rows
        assert payload_rows == total_rows
        cur.execute(
            """
            SELECT av.attrs, av.raw_attrs
              FROM okg.attribute_effective_view_intervals iv
              JOIN okg.attribute_versions av
                USING (target_kind, target_id, attrs_hash)
             WHERE iv.target_id = %s
             LIMIT 1
            """,
            (NODE,),
        )
        attrs, raw_attrs = cur.fetchone()
        assert isinstance(attrs, dict)
        assert raw_attrs is None or isinstance(raw_attrs, dict)
        # Keep the Chronos text segment deliberately stale while changing the
        # content-addressed payload. The lexical supplement must find this row
        # through the interval+version join; a segment-only query cannot.
        cur.execute(
            """
            UPDATE okg.attribute_versions
               SET attrs = jsonb_set(attrs, '{text}', '"interval-only-token"'::jsonb)
             WHERE target_kind = 'node' AND target_id = %s
            """,
            (NODE,),
        )
        assert cur.rowcount >= 1
        cur.execute(
            "SELECT count(*) FROM okg.generation_ordinals "
            "WHERE checkpoint_id = ANY(%s)",
            ([first, second],),
        )
        assert cur.fetchone()[0] == 2
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    server = build_server(dsn=clean_runtime, deployment_name="default")
    seen_sql: list[str] = []
    original_execute = psycopg.Cursor.execute
    original_stream = psycopg.Cursor.stream

    def capture_execute(self, query, params=None, **kwargs):
        seen_sql.append(" ".join(str(query).split()))
        return original_execute(self, query, params, **kwargs)

    def capture_stream(self, query, params=None, **kwargs):
        seen_sql.append(" ".join(str(query).split()))
        return original_stream(self, query, params, **kwargs)

    monkeypatch.setattr(psycopg.Cursor, "execute", capture_execute)
    monkeypatch.setattr(psycopg.Cursor, "stream", capture_stream)

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
            lexical = await session.call_tool(
                "search",
                {
                    "query": "interval-only-token",
                    "method": "lexical",
                    "attr_filters": {"version": 2},
                    "subtype_filter": "document_chunk",
                    "select": ["node_id", "subtype", "attrs", "score"],
                    "generation_id": second,
                },
            )
            for result in (attrs_lookup, lexical):
                assert result.isError is False, _result_text(result)
            attrs_payload = _result_payload(attrs_lookup)
            lexical_payload = _result_payload(lexical)
            assert attrs_payload["pinned_generation_id"] == first
            assert [row["node_id"] for row in attrs_payload["results"]] == [NODE]
            assert attrs_payload["results"][0]["attrs"]["version"] == 1
            assert lexical_payload["pinned_generation_id"] == second
            assert [row["node_id"] for row in lexical_payload["results"]] == [NODE]
            assert lexical_payload["results"][0]["attrs"]["version"] == 2
            assert "interval-only-token" in lexical_payload["results"][0]["attrs"]["text"]

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

        interval_reads = [
            statement
            for statement in seen_sql
            if "attribute_effective_view_intervals" in statement
        ]
        assert interval_reads, "the default MCP reads did not use interval view"
        assert any(
            "attribute_versions" in statement for statement in interval_reads
        ), interval_reads
        assert any(
            "attribute_effective_view_intervals v JOIN okg.attribute_versions av"
            in statement
            for statement in interval_reads
        ), interval_reads
        assert any(
            "USING (target_kind, target_id, attrs_hash)" in statement
            for statement in interval_reads
        ), interval_reads

        # Snapshot mode is a supported alternate only because this fixture has
        # a real materialized snapshot. It must return the same pinned answer.
        monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")
        async with create_connected_server_and_client_session(server) as session:
            snapshot = await session.call_tool(
                "search",
                {
                    "method": "attrs",
                    "attr_filters": {"version": 1},
                    "subtype_filter": "document_chunk",
                    "generation_id": first,
                },
            )
            assert snapshot.isError is False, snapshot
            payload = _result_payload(snapshot)
            assert payload["pinned_generation_id"] == first
            assert [row["node_id"] for row in payload["results"]] == [NODE]
            assert payload["results"][0]["attrs"]["version"] == 1

    try:
        anyio.run(drive)
    finally:
        server._okg_pool.close()
        if server._okg_write_pool:
            server._okg_write_pool.close()


def test_issue_1186_snapshot_mode_refuses_missing_relation(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")
    server = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        with pytest.raises(SubstrateError, match="attribute_effective_view is missing"):
            server._okg_effective_view_source(  # type: ignore[attr-defined]
                _MissingSnapshotConnection(), "generation-1186", target_kind="node"
            )
    finally:
        server._okg_pool.close()
        if server._okg_write_pool:
            server._okg_write_pool.close()


def test_issue_1186_interval_mode_refuses_missing_relation(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    server = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        with pytest.raises(
            SubstrateError,
            match="attribute_effective_view_intervals is missing",
        ):
            server._okg_effective_view_source(  # type: ignore[attr-defined]
                _MissingSnapshotConnection(), "generation-1186", target_kind="node"
            )
    finally:
        server._okg_pool.close()
        if server._okg_write_pool:
            server._okg_write_pool.close()


def test_issue_1186_interval_mode_refuses_missing_payload_join(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _first, second = _publish_two_generations(clean_runtime)

    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    server = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        with pytest.raises(SubstrateError, match="incomplete attrs_hash payload join"):
            server._okg_effective_view_source(  # type: ignore[attr-defined]
                _IncompletePayloadConnection(), second, target_kind="node"
            )
    finally:
        server._okg_pool.close()
        if server._okg_write_pool:
            server._okg_write_pool.close()
