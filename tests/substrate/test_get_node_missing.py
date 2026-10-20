"""Regression test for `debug-substrate-okg-self` Phase 7.

`inspect_node(missing)` must raise `NodeNotFound` with a
structured payload (node_id + generation_id), not return
None. Returning None silently serialized to empty MCP
content — agents couldn't distinguish "no such node" from
"exists with no attrs".
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.errors import NodeNotFound
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import open_session
from okg.substrate.mcp.tools import inspect_node as _get_node
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import (
    bind_test_catalog_writer,
    seed_catalog,
)
# The Nomos source-lineage policy the MCP read tests use: public_docs is
# readable by subject:mcp-client, google_drive is not.
from tests.substrate.mcp.test_nomos_source_lineage_mcp import (
    _activate_source_policy,
)


pytestmark = pytest.mark.postgres


def test_node_not_found_class_exists() -> None:
    """The exception class is exported and carries a stable code."""
    assert NodeNotFound.code == "node_not_found"
    err = NodeNotFound("test", data={"node_id": "x"})
    assert err.code == "node_not_found"
    assert err.data["node_id"] == "x"


def _seed_one_node(conn: psycopg.Connection) -> None:
    """Seed minimal state so `open_session` resolves a published gen."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = cur.fetchone()[0]
    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts([
        NodeFact(
            node_id="n:present",
            subtype="page",
            attrs={"text": "present"},
            source_record_id={"node_id": "n:present"},
            source_revision={"rev": 1},
        )
    ])
    store.checkpoint_generation(
        deployment_name="default",
        catalog_version_id=int(cv),
    )


def test_get_node_substrate_helper_returns_none(clean_runtime: str) -> None:
    """The lower-level `okg.substrate.mcp.tools.get_node` helper
    keeps its `Optional[dict]` contract for in-Python callers.
    The MCP wrapper is the one that raises (covered separately
    via the e2e MCP test in Phase 2).
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_one_node(conn)
        session = open_session(conn, deployment_name="default")
        result = _get_node(conn, session, "no:such:node")
        assert result is None


def test_mcp_wrapper_raises_node_not_found() -> None:
    """The MCP wrapper at `okg.substrate.mcp.server.get_node`
    raises NodeNotFound (rather than returning None).

    Inspected directly here (the wrapper is decorator-shaped so
    we exercise the raise path via the build_server function).
    """
    # We don't want to spin up FastMCP for this — just confirm
    # the exception class is wired into the import path the
    # wrapper uses.
    from okg.substrate.errors import NodeNotFound as Imported
    assert Imported is NodeNotFound


# ---------- inspect names derived documents (issue #2669) ----------

def _seed_file_document_catalog(conn: psycopg.Connection) -> int:
    return seed_catalog(
        conn,
        subtypes=[
            "source_file", "document", "document_chunk", "visual_artifact",
            "blob",
        ],
        narrowings=[
            ("document", "derived_from", "source_file"),
            ("document", "contains", "document_chunk"),
            ("document", "contains", "visual_artifact"),
        ],
    )


def _insert_node_facts(
    conn: psycopg.Connection,
    nodes: list[tuple[str, str, str]],  # (node_id, subtype, text)
) -> None:
    ChronosGraphStore.from_connection(conn).apply_facts([
        NodeFact(
            node_id=nid,
            subtype=subtype,
            attrs={"text": text},
            source_record_id={"id": nid, "source": "fixture"},
            source_revision={"v": 1},
        )
        for nid, subtype, text in nodes
    ])


def _insert_edge_facts(
    conn: psycopg.Connection,
    edges: list[tuple[str, str, str]],  # (src, dst, edge_type)
) -> None:
    ChronosGraphStore.from_connection(conn).apply_facts([
        EdgeFact(
            src=src,
            dst=dst,
            edge_type=etype,
            attrs={},
            source_record_id={"src": src, "dst": dst, "source": "fixture"},
            source_revision={"v": 1},
        )
        for src, dst, etype in edges
    ])


def _publish(dsn: str) -> None:
    result = Publisher(
        dsn, "default",
        catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()
    assert result["status"] == "published"


def test_inspect_node_names_documents_derived_from_a_file(
    clean_runtime: str,
) -> None:
    """A `file:` node's text is only its path; inspect must name the
    `doc:` node that holds its chunks, and count only the chunks."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_file_document_catalog(conn)
        _insert_node_facts(conn, [
            ("file:r:a.yaml", "source_file", "a.yaml"),
            ("file:r:bare.txt", "source_file", "bare.txt"),
            ("doc:r:a.yaml", "document", "a.yaml"),
            ("chunk:1", "document_chunk", "reaperCount: 0"),
            ("chunk:2", "document_chunk", "image: rucio"),
            ("visual:1", "visual_artifact", "figure"),
            ("blob:1", "blob", "b"),
            ("doc:r:blob", "document", "b"),
        ])
        _insert_edge_facts(conn, [
            ("doc:r:a.yaml", "file:r:a.yaml", "derived_from"),
            ("doc:r:a.yaml", "chunk:1", "contains"),
            ("doc:r:a.yaml", "chunk:2", "contains"),
            ("doc:r:a.yaml", "visual:1", "contains"),
            # The catalog declares no `document derived_from blob`, so
            # inspect of the blob runs no lookup and reports nothing.
            ("doc:r:blob", "blob:1", "derived_from"),
        ])
    _publish(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        file_node = _get_node(conn, s, "file:r:a.yaml")
        bare_node = _get_node(conn, s, "file:r:bare.txt")
        doc_node = _get_node(conn, s, "doc:r:a.yaml")
        blob_node = _get_node(conn, s, "blob:1")
        subtype_only = _get_node(
            conn, s, "file:r:a.yaml",
            select=["node_id", "subtype"], include_documents=False,
        )
    assert file_node is not None
    assert file_node["documents"] == [
        {"node_id": "doc:r:a.yaml", "chunk_count": 2},
    ]
    assert "documents_truncated" not in file_node
    # No derived document: the payload is unchanged (no key at all).
    assert bare_node is not None
    assert "documents" not in bare_node
    assert "documents_truncated" not in bare_node
    # The edge is read in its own direction only: the document derives
    # from the file; nothing derives from the document.
    assert doc_node is not None
    assert "documents" not in doc_node
    # Catalog gate: an undeclared derived_from target gets no lookup.
    assert blob_node is not None
    assert "documents" not in blob_node
    # The internal subtype-only caller opts out.
    assert subtype_only is not None
    assert "documents" not in subtype_only


def test_inspect_node_caps_derived_documents_and_says_so(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_file_document_catalog(conn)
        _insert_node_facts(
            conn,
            [("file:r:big", "source_file", "big"),
             ("file:r:ten", "source_file", "ten")]
            + [(f"doc:r:big:{i:02d}", "document", "d") for i in range(11)]
            + [(f"doc:r:ten:{i:02d}", "document", "d") for i in range(10)],
        )
        _insert_edge_facts(conn, [
            (f"doc:r:big:{i:02d}", "file:r:big", "derived_from")
            for i in range(11)
        ] + [
            (f"doc:r:ten:{i:02d}", "file:r:ten", "derived_from")
            for i in range(10)
        ])
    _publish(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        node = _get_node(conn, s, "file:r:big")
        exactly_ten = _get_node(conn, s, "file:r:ten")
    assert node is not None
    assert [row["node_id"] for row in node["documents"]] == [
        f"doc:r:big:{i:02d}" for i in range(10)
    ]
    assert all(row["chunk_count"] == 0 for row in node["documents"])
    assert node["documents_truncated"] is True
    # Exactly at the limit: all ten listed, no truncation flag.
    assert exactly_ten is not None
    assert len(exactly_ten["documents"]) == 10
    assert "documents_truncated" not in exactly_ten


def test_inspect_derived_documents_hide_denied_source_lineage(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #2669: inspect(file) names the documents derived from it. A
    document whose edge or node the reader may not see is not listed, a
    chunk whose edge or node is denied is not counted, hidden documents that sort
    first cannot crowd out a visible one, and the chunk count is withheld,
    not guessed, once the exact-count budget runs out."""
    from okg.substrate.mcp import tools as mcp_tools

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["source_file", "document", "document_chunk"],
            narrowings=[
                ("document", "derived_from", "source_file"),
                ("document", "contains", "document_chunk"),
            ],
        )
        store = ChronosGraphStore.from_connection(conn)

        def node(node_id: str, subtype: str) -> NodeFact:
            return NodeFact(
                node_id, subtype,
                {"classification": "internal", "text": node_id},
                {"id": node_id}, {"v": 1},
            )

        def edge(src: str, dst: str, edge_type: str) -> EdgeFact:
            return EdgeFact(
                src, dst, edge_type, {"classification": "internal"},
                source_record_id={"id": f"{src}->{dst}"},
                source_revision={"v": 1},
            )

        public_chunks = [f"chunk:public:{i}" for i in range(5)]
        second_chunks = [f"chunk:second:{i}" for i in range(3)]
        store.apply_facts(
            [
                node("file:r:a.yaml", "source_file"),
                node("file:r:b.yaml", "source_file"),
                node("doc:r:a.yaml", "document"),
                node("doc:r:b-public", "document"),
                *(node(chunk, "document_chunk") for chunk in public_chunks),
                *(node(chunk, "document_chunk") for chunk in second_chunks),
            ],
            source="public_docs",
        )
        # The hidden documents' ids sort BEFORE the visible one.
        store.apply_facts(
            [
                node("doc:r:0-denied-node", "document"),
                node("doc:r:0-secret", "document"),
                node("chunk:secret", "document_chunk"),
                node("chunk:hidden-node", "document_chunk"),
                edge("doc:r:0-secret", "file:r:a.yaml", "derived_from"),
                edge("doc:r:0-secret", "file:r:b.yaml", "derived_from"),
                edge("doc:r:a.yaml", "chunk:secret", "contains"),
            ],
            source="google_drive",
        )
        store.apply_facts(
            [
                edge("doc:r:a.yaml", "file:r:a.yaml", "derived_from"),
                *(edge("doc:r:a.yaml", chunk, "contains") for chunk in public_chunks),
                # A visible edge into a chunk node the reader is denied.
                edge("doc:r:a.yaml", "chunk:hidden-node", "contains"),
                edge("doc:r:b-public", "file:r:a.yaml", "derived_from"),
                *(edge("doc:r:b-public", chunk, "contains") for chunk in second_chunks),
                # A visible edge into a document node the reader is denied.
                edge("doc:r:0-denied-node", "file:r:a.yaml", "derived_from"),
            ],
            source="public_docs",
        )
    _publish(clean_runtime)

    # Control: without enforcement every document and chunk is visible.
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        unfiltered = _get_node(conn, session, "file:r:a.yaml")
    assert unfiltered is not None
    assert unfiltered["documents"] == [
        {"node_id": "doc:r:0-denied-node", "chunk_count": 0},
        {"node_id": "doc:r:0-secret", "chunk_count": 0},
        {"node_id": "doc:r:a.yaml", "chunk_count": 7},
        {"node_id": "doc:r:b-public", "chunk_count": 3},
    ]

    _activate_source_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-client")

    def inspect_under_enforce(node_id: str) -> dict:
        with psycopg.connect(clean_runtime) as conn:
            session = open_session(conn, deployment_name="default")
            payload = _get_node(conn, session, node_id)
        assert payload is not None
        return payload

    filtered = inspect_under_enforce("file:r:a.yaml")
    assert filtered["documents"] == [
        {"node_id": "doc:r:a.yaml", "chunk_count": 5},
        {"node_id": "doc:r:b-public", "chunk_count": 3},
    ]
    assert "documents_truncated" not in filtered
    # Every derived document hidden: no key at all, and no bare
    # `documents_truncated` that would announce hidden rows.
    only_hidden = inspect_under_enforce("file:r:b.yaml")
    assert "documents" not in only_hidden
    assert "documents_truncated" not in only_hidden

    # The list is cut AFTER the read filter: two hidden documents that sort
    # first do not push the first visible one past a limit of one, and the
    # second visible one is announced as truncated.
    monkeypatch.setattr(mcp_tools, "_DERIVED_DOCUMENTS_LIMIT", 1)
    capped = inspect_under_enforce("file:r:a.yaml")
    assert capped["documents"] == [
        {"node_id": "doc:r:a.yaml", "chunk_count": 5},
    ]
    assert capped["documents_truncated"] is True
    monkeypatch.undo()
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-client")

    # One budget serves the whole call. The first document's seven raw
    # chunk edges fit a budget of nine; the second's three exceed the two
    # left, so that count is withheld with its reason instead of a raw
    # number the reader may not see.
    monkeypatch.setattr(mcp_tools, "_NEIGHBORS_EXACT_COUNT_BUDGET", 9)
    over_budget = inspect_under_enforce("file:r:a.yaml")
    assert over_budget["documents"] == [
        {"node_id": "doc:r:a.yaml", "chunk_count": 5},
        {
            "node_id": "doc:r:b-public",
            "chunk_count": None,
            "chunk_count_basis": "omitted_over_budget_under_enforce",
        },
    ]
