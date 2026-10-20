from __future__ import annotations

import json
import re
import uuid
from pathlib import Path

import psycopg
import pytest

from okg.substrate.errors import NodeNotFound
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.extraction._protocols import ParsedChunk, ParsedDocument
from okg.substrate.library.extraction.chunk.markdown_heading import MarkdownHeadingChunker
from okg.substrate.library.extraction.chunk.window import WindowChunker
from okg.substrate.library.extraction.parse.markdown import MarkdownParser
from okg.substrate.library.extraction.parse.text import TextParser
from okg.substrate.library.extraction.registry import ExtractorBundle
from okg.substrate.library.sources import doc_corpus as doc_corpus_module
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.doc_corpus import DocCorpusSource, _chunk_node_id
from okg.substrate.mcp.continuation import ContinuationTokenInvalid
from okg.substrate.mcp.path_diagnostics import InvalidExpandShape
from okg.substrate.mcp.response_bounds import public_call_tool_result_bytes
from okg.substrate.mcp.run_sql import _expand_composite_views, _rewrite_graph_views
from okg.substrate.mcp.server import build_server
from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import SubtypeSpec, seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


class _OccurrenceParser:
    def parse(self, content: bytes | str, **_: object) -> ParsedDocument:
        text = content.decode() if isinstance(content, bytes) else content
        body = "SHARED OCCURRENCE"
        start = text.index(body)
        label = text.splitlines()[0]
        return ParsedDocument(
            text=text,
            chunks=[ParsedChunk(
                body,
                heading_path=[label],
                span=(start, start + len(body)),
            )],
        )


def test_member_edge_keeps_each_documents_occurrence_context(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "a.txt").write_text("A\nSHARED OCCURRENCE")
    (corpus / "b.txt").write_text("LONG HEADING B\n....SHARED OCCURRENCE")
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    (extractors / "extractors.yaml").write_text("{}\n")

    bundle = ExtractorBundle(
        parsers={"text": _OccurrenceParser()},
        chunker=None,
        ner=None,
    )
    monkeypatch.setattr(doc_corpus_module, "load_extractors", lambda _path: bundle)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    SourceRunner(clean_runtime, source_name="test.typed-occurrences").run(
        DocCorpusSource(
            repo="test",
            repo_root=corpus, non_git_walk=True,
            extractors_dir=extractors,
        ),
        mode="scope_complete",
    )
    published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert published["status"] == "published"

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        node_rows = store.query(
            "SELECT node_id, attrs FROM okg.graph_nodes "
            "WHERE source = 'test.typed-occurrences' "
            "AND subtype = 'document_chunk'"
        )
        assert [row["node_id"] for row in node_rows] == [
            _chunk_node_id("SHARED OCCURRENCE")
        ]

        edge_rows = store.query(
            "SELECT src, dst, edge_type, attrs FROM okg.graph_edges "
            "WHERE source = 'test.typed-occurrences' ORDER BY edge_type, dst"
        )
        member_rows = [row for row in edge_rows if row["edge_type"] == "member_of"]
        assert len(member_rows) == 2, edge_rows
        generation = store.checkout_generation(published["generation_id"])
        view_sql = (
            "SELECT path, chunk_kind, char_offset, char_end, char_length, "
            "heading_path FROM okg.v_doc_chunks "
            "WHERE repo = 'test' ORDER BY path"
        )
        with psycopg.connect(clean_runtime) as conn:
            expanded = _expand_composite_views(view_sql, conn)
        graph_sql = _rewrite_graph_views(expanded)
        assert "graph_nodes" in graph_sql and "graph_edges" in graph_sql
        rows = generation.query(graph_sql)
        assert rows == [
            {
                "path": "a.txt",
                "chunk_kind": "paragraph",
                "char_offset": 2,
                "char_end": 19,
                "char_length": 17,
                "heading_path": "A",
            },
            {
                "path": "b.txt",
                "chunk_kind": "paragraph",
                "char_offset": 19,
                "char_end": 36,
                "char_length": 17,
                "heading_path": "LONG HEADING B",
            },
        ]
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# Issue #2668: every occurrence carries its emission position, and
# inspect(target={"kind":"text"}) reads a whole record in that order.
# --------------------------------------------------------------------------- #


def _ingest(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    files: dict[str, str],
    bundle: ExtractorBundle,
    source_name: str,
    subtypes: list[SubtypeSpec] | None = None,
) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    for name, body in files.items():
        (corpus / name).write_text(body)
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    (extractors / "extractors.yaml").write_text("{}\n")
    monkeypatch.setattr(doc_corpus_module, "load_extractors", lambda _path: bundle)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        if subtypes is None:
            seed_catalog_version(conn)
        else:
            seed_catalog(conn, subtypes=subtypes)
    SourceRunner(clean_runtime, source_name=source_name).run(
        DocCorpusSource(
            repo="test", repo_root=corpus, non_git_walk=True,
            extractors_dir=extractors,
        ),
        mode="scope_complete",
    )
    _publish(clean_runtime)


def _publish(clean_runtime: str) -> None:
    published = Publisher(
        clean_runtime,
        "default",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert published["status"] == "published", published


def _doc_id(clean_runtime: str, source_name: str, path: str) -> str:
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        rows = store.query(
            "SELECT node_id FROM okg.graph_nodes WHERE subtype = 'document' "
            "AND source = :source AND attrs->>'path' = :path",
            {"source": source_name, "path": path},
        )
    finally:
        store.close()
    assert len(rows) == 1, rows
    return str(rows[0]["node_id"])


def _read_text(mcp, node_id: str, *, max_bytes: int | None = None) -> list[dict]:
    """Follow the text read's cursor to the end through the served tool."""
    tool = mcp._tool_manager._tools["inspect"]
    pages: list[dict] = []
    token = None
    for _ in range(200):
        target = {"kind": "text", "node_id": node_id}
        if token is not None:
            target["continuation_token"] = token
        kwargs = {} if max_bytes is None else {"max_bytes": max_bytes}
        page = tool.fn(target=target, **kwargs)
        assert page.get("status") == "ok", page
        # What the agent is actually sent, freshness stamps included, fits.
        assert public_call_tool_result_bytes(page) <= (max_bytes or 64 * 1024)
        pages.append(page)
        if page["complete"]:
            return pages
        token = page["continuation_token"]
    raise AssertionError("text read cursor did not terminate")


_FENCE_OPEN = re.compile(r"\A⟦UNTRUSTED_DATA source=[^⟧]*⟧\n")
_FENCE_CLOSE = "\n⟦/UNTRUSTED_DATA⟧"


def _unfenced(item: dict) -> str:
    """The segment text without its untrusted-data fence.

    Fixture sources have no producer profile, so every segment is served
    fenced; that is the read-side safety boundary working, and the fence is
    asserted rather than assumed."""
    text = item["text"]
    assert item["provenance"]["trust"] == "untrusted"
    assert _FENCE_OPEN.match(text) and text.endswith(_FENCE_CLOSE), text[:80]
    return _FENCE_OPEN.sub("", text)[: -len(_FENCE_CLOSE)]


_PARAGRAPHS = [
    f"Paragraph {i:02d} says something distinct about item {i}." for i in range(14)
]
_REPEAT = "Thanks, the operations team."


def test_markdown_occurrences_carry_ordinal_and_text_reads_in_that_order(
    clean_runtime: str,
    okg_mcp_can_log_in: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Parser-split markdown chunks carry no span, so they store no
    char_offset (before reparse epoch 4 they stored a fake 0); the ordinal
    orders them. A repeated paragraph collapses to one edge, and the read says
    the record is partial instead of hiding it."""
    body = _PARAGRAPHS[:2] + [_REPEAT] + _PARAGRAPHS[2:] + [_REPEAT]
    source_name = "test.text-read-markdown"
    _ingest(
        clean_runtime,
        tmp_path,
        monkeypatch,
        files={"ticket.md": "# Ticket\n\n" + "\n\n".join(body) + "\n"},
        bundle=ExtractorBundle(
            parsers={"markdown": MarkdownParser()}, chunker=None, ner=None
        ),
        source_name=source_name,
    )
    doc_id = _doc_id(clean_runtime, source_name, "ticket.md")

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        edges = store.query(
            "SELECT attrs FROM okg.graph_edges WHERE edge_type = 'member_of' "
            "AND dst = :doc",
            {"doc": doc_id},
        )
    finally:
        store.close()
    ordinals = sorted(int(e["attrs"]["chunk_ordinal"]) for e in edges)
    assert {int(e["attrs"]["chunk_total"]) for e in edges} == {len(body)}
    # Sixteen occurrences, fifteen edges: the repeat kept one ordinal.
    assert len(edges) == len(body) - 1
    assert len(set(ordinals)) == len(ordinals)
    for edge in edges:
        for key in ("char_offset", "char_end", "char_length", "doc_char_length"):
            assert key not in edge["attrs"], (key, edge["attrs"])

    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    pages = _read_text(mcp, doc_id)
    assert len(pages) == 1
    page = pages[0]
    assert page["order_basis"] == "chunk_ordinal"
    assert page["coverage"] == "partial"
    texts = [_unfenced(item) for item in page["items"]]
    keep_first = body[:-1]
    keep_last = body[:2] + body[3:]
    assert texts in (keep_first, keep_last), texts


def test_windowed_text_reads_whole_in_one_call_and_pages_by_bytes(
    clean_runtime: str,
    okg_mcp_can_log_in: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 30-chunk plain-text record: one call at the default budget, overlap
    trimmed so the segments rejoin to the source, and the same text again
    when a small budget forces paging."""
    text = "".join(
        f"Log line {i:05d}: the job moved to the next state.\n" for i in range(800)
    )
    source_name = "test.text-read-window"
    _ingest(
        clean_runtime,
        tmp_path,
        monkeypatch,
        files={"ticket.txt": text, "other.txt": text[::-1]},
        bundle=ExtractorBundle(
            parsers={"text": TextParser()},
            chunker=None,
            ner=None,
            chunkers_by_format={
                "default": WindowChunker(max_chars=1500, overlap=200)
            },
        ),
        source_name=source_name,
    )
    doc_id = _doc_id(clean_runtime, source_name, "ticket.txt")
    mcp = build_server(dsn=clean_runtime, deployment_name="default")

    whole = _read_text(mcp, doc_id)
    assert len(whole) == 1
    items = whole[0]["items"]
    assert len(items) >= 30
    assert whole[0]["order_basis"] == "chunk_ordinal"
    assert whole[0]["coverage"] == "complete"
    assert "".join(_unfenced(item) for item in items) == text
    assert all(item.get("trimmed_chars") == 200 for item in items[1:])

    paged = _read_text(mcp, doc_id, max_bytes=8192)
    assert len(paged) > 3
    assert "".join(_unfenced(i) for p in paged for i in p["items"]) == text

    # A cursor is bound to its record: replayed on another one it is refused.
    other_id = _doc_id(clean_runtime, source_name, "other.txt")
    with pytest.raises(ContinuationTokenInvalid):
        mcp._tool_manager._tools["inspect"].fn(
            target={
                "kind": "text",
                "node_id": other_id,
                "continuation_token": paged[0]["continuation_token"],
            },
            max_bytes=8192,
        )


# Under Nomos enforce: hidden chunks are omitted, redacted ones keep their
# marker, and the pages a caller sees do not depend on rows it cannot see.


def _chunk_text(i: int) -> str:
    return f"Visible section {i:02d}. " + ("detail " * 300)


def _enforce_policy() -> dict:
    return {
        "policy_bundle_id": "nomos:test:text-read",
        "policy_version": "v1",
        "default_effect": "allow",
        "rules": [
            {
                "rule_id": "hide_secret_mcp_read",
                "effect": "deny",
                "priority": 300,
                "subjects": ["subject:mcp-reader"],
                "actions": ["graph.read.mcp"],
                "match": {"classification": "secret"},
            },
            {
                "rule_id": "redact_restricted_mcp_read",
                "effect": "redact",
                "priority": 200,
                "subjects": ["subject:mcp-reader"],
                "actions": ["graph.read.mcp"],
                "match": {"classification": "restricted"},
                "obligations": [{"type": "redaction"}],
            },
            {
                "rule_id": "allow_mcp_read",
                "effect": "allow",
                "priority": 100,
                "subjects": ["subject:mcp-reader"],
                "actions": ["graph.read.mcp"],
            },
        ],
    }


def _record_facts(
    doc: str,
    ordinals: list[int],
    *,
    total: int,
    secret: set[int],
    restricted: set[int],
    with_doc: bool = True,
) -> list:
    facts: list = []
    if with_doc:
        facts.append(
            NodeFact(
                doc,
                "document",
                {"classification": "internal", "path": f"{doc}.md"},
                {"id": doc},
                {"v": 1},
            )
        )
    for i in ordinals:
        chunk = f"chunk:{doc}:{i:02d}"
        classification = (
            "secret" if i in secret else "restricted" if i in restricted else "internal"
        )
        facts.append(
            NodeFact(
                chunk,
                "document_chunk",
                {
                    "classification": classification,
                    "text": _chunk_text(i),
                    "chunk_kind": "paragraph",
                },
                {"id": chunk},
                {"v": 1},
            )
        )
        facts.append(
            EdgeFact(
                chunk,
                doc,
                "member_of",
                {
                    "classification": classification,
                    "chunk_ordinal": i,
                    "chunk_total": total,
                },
                source_record_id={"id": f"{chunk}-m"},
                source_revision={"v": 1},
            )
        )
        facts.append(
            EdgeFact(
                doc,
                chunk,
                "contains",
                {"classification": classification},
                source_record_id={"id": f"{chunk}-c"},
                source_revision={"v": 1},
            )
        )
    return facts


def _seed_enforce_catalog(conn: psycopg.Connection) -> None:
    seed_catalog(
        conn,
        subtypes=[
            SubtypeSpec("document"),
            SubtypeSpec(
                "document_chunk",
                metadata={
                    "pii_classes": {"classification": "non_pii", "text": "free_text"}
                },
            ),
        ],
        narrowings=[
            ("document", "contains", "document_chunk"),
            ("document_chunk", "member_of", "document"),
        ],
    )
    activate_policy_bundle(
        conn,
        deployment_name="default",
        bundle=compile_policy_graph(_enforce_policy()),
        scope="deployment:default",
        activated_by="test:operator",
        bootstrap=True,
    )


def _page_shape(pages: list[dict]) -> list[list[tuple]]:
    return [
        [
            (i["chunk_id"], i.get("chunk_ordinal"), i["text"], i.get("redacted", False))
            for i in p["items"]
        ]
        + [("complete", p["complete"]), ("coverage", p["coverage"])]
        for p in pages
    ]


def test_enforce_omits_hidden_chunks_keeps_redaction_and_pages_ignore_hidden_rows(
    clean_runtime: str,
    okg_mcp_can_log_in: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    doc = f"doc:{sfx}"
    hidden_doc = f"doc:{sfx}-all-hidden"
    empty_doc = f"doc:{sfx}-empty"
    visible = [0, 1, 2, 4, 5, 7, 8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_enforce_catalog(conn)
        # Generation 1: only rows the reader may see (4 is redacted).
        ChronosGraphStore.from_connection(conn).apply_facts(
            _record_facts(doc, visible, total=10, secret=set(), restricted={4})
            + _record_facts(hidden_doc, [0, 1], total=2, secret={0, 1}, restricted=set())
            + _record_facts(empty_doc, [], total=0, secret=set(), restricted=set()),
            source="default",
            strict_endpoints=True,
        )
    _publish(clean_runtime)

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-reader")
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    before = _read_text(mcp, doc, max_bytes=6144)
    assert len(before) > 2, "the fixture must page for this test to mean anything"

    # Generation 2: hidden chunks interleaved (3, 6) and trailing (9).
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            _record_facts(
                doc, [3, 6, 9], total=10, secret={3, 6, 9}, restricted=set(),
                with_doc=False,
            ),
            source="default",
            strict_endpoints=True,
        )
    _publish(clean_runtime)
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    after = _read_text(mcp, doc, max_bytes=6144)

    assert _page_shape(after) == _page_shape(before)
    items = [i for p in after for i in p["items"]]
    assert [int(i["chunk_ordinal"]) for i in items] == visible
    assert [int(i["chunk_ordinal"]) for i in items if i.get("redacted")] == [4]
    assert "Visible section 04" not in json.dumps(after)
    # The only counts in the answer are edge attributes a one-hop expand
    # already shows the caller (chunk_total); the read adds none of its own.
    assert all("total_count" not in p and "total" not in p for p in after)
    # Every chunk hidden answers exactly as no chunks at all.
    all_hidden = _read_text(mcp, hidden_doc)[0]
    no_chunks = _read_text(mcp, empty_doc)[0]
    answer_keys = (
        "status", "subtype", "order_basis", "coverage", "items", "complete",
        "truncated", "outcome", "continuation_token",
    )
    assert {k: all_hidden.get(k) for k in answer_keys} == {
        k: no_chunks.get(k) for k in answer_keys
    }
    assert all_hidden["outcome"]["code"] == "no_text_chunks"

    # The same read with enforcement off returns every chunk.
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    everything = [i for p in _read_text(mcp, doc) for i in p["items"]]
    assert [int(i["chunk_ordinal"]) for i in everything] == list(range(10))


def test_record_without_chunks_and_unknown_record(
    clean_runtime: str,
    okg_mcp_can_log_in: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    empty_doc = f"doc:{sfx}-empty"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_enforce_catalog(conn)
        ChronosGraphStore.from_connection(conn).apply_facts(
            _record_facts(empty_doc, [], total=0, secret=set(), restricted=set()),
            source="default",
            strict_endpoints=True,
        )
    _publish(clean_runtime)
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    page = _read_text(mcp, empty_doc)[0]
    assert page["items"] == []
    assert page["outcome"]["code"] == "no_text_chunks"
    with pytest.raises(NodeNotFound):
        mcp._tool_manager._tools["inspect"].fn(
            target={"kind": "text", "node_id": f"doc:{sfx}-not-there"}
        )
    # The one-hop shape agents tried in the benchmark names the call that works.
    # Since #3666 a one-hop row is the linked record and `target_subtype` is
    # served there as the type condition; the refusal and its pointer remain
    # on link rows (a `select` of only the link's own fields).
    with pytest.raises(InvalidExpandShape) as refused:
        mcp._tool_manager._tools["expand"].fn(
            input={"node_id": empty_doc}, target_subtype="document_chunk",
            select=["edge_id", "src", "dst"],
        )
    assert '"kind":"text"' in str(refused.value.fix_hint)


def test_text_read_on_a_chunk_returns_its_text_and_only_visible_records(
    clean_runtime: str,
    okg_mcp_can_log_in: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An agent that found a chunk and asked for its text got an empty list.

    The chunk is shared by three records: one the reader may see, one whose
    node is secret, and one whose occurrence edge is secret. The read returns
    the chunk's own text and names only the record the reader may see."""
    sfx = uuid.uuid4().hex[:8]
    doc = f"doc:{sfx}"
    secret_doc = f"doc:{sfx}-secret-node"
    secret_edge_doc = f"doc:{sfx}-secret-edge"
    chunk = f"chunk:{doc}:01"
    hidden_only = f"chunk:{sfx}-hidden-parents-only"
    orphan = f"chunk:{sfx}-orphan"
    loose = {"classification": "internal", "text": "Loose text.", "chunk_kind": "paragraph"}
    extra: list = [
        NodeFact(hidden_only, "document_chunk", loose, {"id": hidden_only}, {"v": 1}),
        NodeFact(orphan, "document_chunk", loose, {"id": orphan}, {"v": 1}),
        EdgeFact(hidden_only, secret_doc, "member_of",
                 {"classification": "internal", "chunk_ordinal": 0},
                 source_record_id={"id": f"{hidden_only}-m-sn"},
                 source_revision={"v": 1}),
        EdgeFact(hidden_only, secret_edge_doc, "member_of",
                 {"classification": "secret", "chunk_ordinal": 0},
                 source_record_id={"id": f"{hidden_only}-m-se"},
                 source_revision={"v": 1}),
        NodeFact(secret_doc, "document", {"classification": "secret"},
                 {"id": secret_doc}, {"v": 1}),
        NodeFact(secret_edge_doc, "document", {"classification": "internal"},
                 {"id": secret_edge_doc}, {"v": 1}),
        EdgeFact(chunk, secret_doc, "member_of",
                 {"classification": "internal", "chunk_ordinal": 0},
                 source_record_id={"id": f"{chunk}-m-sn"}, source_revision={"v": 1}),
        EdgeFact(chunk, secret_edge_doc, "member_of",
                 {"classification": "secret", "chunk_ordinal": 0},
                 source_record_id={"id": f"{chunk}-m-se"}, source_revision={"v": 1}),
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_enforce_catalog(conn)
        ChronosGraphStore.from_connection(conn).apply_facts(
            _record_facts(doc, [0, 1], total=2, secret=set(), restricted=set())
            + extra,
            source="default",
            strict_endpoints=True,
        )
    _publish(clean_runtime)

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-reader")
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    (page,) = _read_text(mcp, chunk)
    assert [i["chunk_id"] for i in page["items"]] == [chunk]
    assert _unfenced(page["items"][0]) == _chunk_text(1)
    assert page["subtype"] == "document_chunk"
    assert page["order_basis"] == "single_chunk"
    assert page["coverage"] == "complete"
    assert page["parents"] == [
        {"node_id": doc, "subtype": "document", "edge_type": "member_of",
         "chunk_ordinal": 1, "chunk_total": 2},
    ]
    assert page["parents_complete"] is True
    assert page["outcome"]["code"] == "single_chunk"
    assert json.dumps(doc) in page["outcome"]["next_move"]
    served = json.dumps(page)
    assert secret_doc not in served and secret_edge_doc not in served

    # A chunk whose every record is hidden answers exactly as one with none.
    keys = ("coverage", "complete", "parents", "parents_complete", "outcome")
    (behind_hidden,) = _read_text(mcp, hidden_only)
    (no_records,) = _read_text(mcp, orphan)
    assert {k: behind_hidden.get(k) for k in keys} == {
        k: no_records.get(k) for k in keys
    }
    assert behind_hidden["parents"] == [] and behind_hidden["parents_complete"] is True

    # The refusal the agent hit first now names the id it was given.
    with pytest.raises(Exception) as refused:
        mcp._tool_manager._tools["inspect"].fn(
            target={"kind": "node", "node_id": chunk}, select=["text"]
        )
    assert getattr(refused.value, "code", None) == "select_unknown_field"
    hint = str(refused.value.fix_hint)
    assert '{"kind":"text","node_id":' + json.dumps(chunk) + "}" in hint


# --------------------------------------------------------------------------- #
# Issue #3002: under Nomos audit and enforce a read's later pages reuse the
# text its first page filtered, per reader and per policy.
# --------------------------------------------------------------------------- #


def _reader_policy(version: str, *, a_hides_restricted: bool = False) -> dict:
    """reader-a may see every chunk; reader-b may not see secret ones."""
    rules: list[dict] = [
        {
            "rule_id": "hide_secret_from_b",
            "effect": "deny",
            "priority": 300,
            "subjects": ["subject:reader-b"],
            "actions": ["graph.read.mcp"],
            "match": {"classification": "secret"},
        },
        {
            "rule_id": "allow_mcp_read",
            "effect": "allow",
            "priority": 100,
            "subjects": ["subject:reader-a", "subject:reader-b"],
            "actions": ["graph.read.mcp"],
        },
    ]
    if a_hides_restricted:
        rules.append({
            "rule_id": "hide_restricted_from_a",
            "effect": "deny",
            "priority": 250,
            "subjects": ["subject:reader-a"],
            "actions": ["graph.read.mcp"],
            "match": {"classification": "restricted"},
        })
    return {
        "policy_bundle_id": "nomos:test:text-read-cache",
        "policy_version": version,
        "default_effect": "allow",
        "rules": rules,
    }


def _activate(conn: psycopg.Connection, policy: dict, *, bootstrap: bool) -> None:
    activate_policy_bundle(
        conn,
        deployment_name="default",
        bundle=compile_policy_graph(policy),
        scope="deployment:default",
        activated_by="test:operator",
        bootstrap=bootstrap,
    )


_CACHE_ORDINALS = list(range(12))
_CACHE_SECRET = {2, 7, 11}
_CACHE_RESTRICTED = {4, 9}


def _seed_cache_record(clean_runtime: str) -> str:
    doc = f"doc:{uuid.uuid4().hex[:8]}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=[
                SubtypeSpec("document"),
                SubtypeSpec(
                    "document_chunk",
                    metadata={
                        "pii_classes": {"classification": "non_pii", "text": "free_text"}
                    },
                ),
            ],
            narrowings=[
                ("document", "contains", "document_chunk"),
                ("document_chunk", "member_of", "document"),
            ],
        )
        _activate(conn, _reader_policy("v1"), bootstrap=True)
        ChronosGraphStore.from_connection(conn).apply_facts(
            _record_facts(
                doc, _CACHE_ORDINALS, total=len(_CACHE_ORDINALS),
                secret=_CACHE_SECRET, restricted=_CACHE_RESTRICTED,
            ),
            source="default",
            strict_endpoints=True,
        )
    _publish(clean_runtime)
    return doc


def _text_page(mcp, node_id: str, token: str | None) -> dict:
    target = {"kind": "text", "node_id": node_id}
    if token is not None:
        target["continuation_token"] = token
    page = mcp._tool_manager._tools["inspect"].fn(target=target, max_bytes=6144)
    assert page.get("status") == "ok", page
    return page


def _ordinals(pages: list[dict]) -> list[int]:
    return [int(i["chunk_ordinal"]) for p in pages for i in p["items"]]


def _as_reader(monkeypatch: pytest.MonkeyPatch, subject: str) -> None:
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", subject)


def test_enforce_cache_keeps_each_readers_view_across_interleaved_reads(
    clean_runtime: str,
    okg_mcp_can_log_in: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reader b (no access to secret chunks), then reader a (access), then b
    again, on one record and generation: each page is exactly its reader's
    view, the same as with the cache off."""
    from okg.substrate.mcp import record_text_cache

    record_text_cache.clear()
    doc = _seed_cache_record(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    mcp = build_server(dsn=clean_runtime, deployment_name="default")

    def whole(subject: str) -> list[dict]:
        _as_reader(monkeypatch, subject)
        pages, token = [], None
        for _ in range(50):
            pages.append(_text_page(mcp, doc, token))
            token = pages[-1].get("continuation_token")
            if token is None:
                return pages
        raise AssertionError("cursor did not terminate")

    # Each reader's reference view, with the cache off.
    monkeypatch.setenv("OKG_RECORD_TEXT_CACHE_ENTRIES", "0")
    ref_b = _page_shape(whole("subject:reader-b"))
    ref_a = _page_shape(whole("subject:reader-a"))
    monkeypatch.setenv("OKG_RECORD_TEXT_CACHE_ENTRIES", "8")
    assert len(ref_b) > 2 and len(ref_a) > 2, "the fixture must page"
    assert ref_a != ref_b

    # b starts, a reads whole while b is mid-read, b finishes, b again.
    _as_reader(monkeypatch, "subject:reader-b")
    b_pages = [_text_page(mcp, doc, None)]
    a_pages = whole("subject:reader-a")
    _as_reader(monkeypatch, "subject:reader-b")
    while (token := b_pages[-1].get("continuation_token")) is not None:
        b_pages.append(_text_page(mcp, doc, token))
    b_again = whole("subject:reader-b")
    a_again = whole("subject:reader-a")

    assert _page_shape(b_pages) == ref_b
    assert _page_shape(b_again) == ref_b
    assert _page_shape(a_pages) == ref_a
    assert _page_shape(a_again) == ref_a
    assert _ordinals(b_pages) == [i for i in _CACHE_ORDINALS if i not in _CACHE_SECRET]
    assert _ordinals(a_pages) == _CACHE_ORDINALS
    for secret in _CACHE_SECRET:
        assert f"Visible section {secret:02d}" not in json.dumps(b_pages + b_again)
    # One entry per reader: the later pages came from the cache.
    assert len(record_text_cache._entries) == 2


def test_enforce_policy_change_is_never_served_from_the_cache(
    clean_runtime: str,
    okg_mcp_can_log_in: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bundle activated mid-read: the old cursor is refused, and a new read
    sees the new policy's view, never the text the old one filtered."""
    from okg.substrate.mcp import record_text_cache

    record_text_cache.clear()
    doc = _seed_cache_record(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    _as_reader(monkeypatch, "subject:reader-a")
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    first = _text_page(mcp, doc, None)
    old_key = next(iter(record_text_cache._entries))

    # Activate v2 (reader a may no longer see restricted chunks). The active
    # policy does not grant policy.activate, so activate under Nomos off.
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _activate(conn, _reader_policy("v2", a_hides_restricted=True), bootstrap=False)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")

    with pytest.raises(ContinuationTokenInvalid):
        _text_page(mcp, doc, first["continuation_token"])
    pages = [_text_page(mcp, doc, None)]
    while (token := pages[-1].get("continuation_token")) is not None:
        pages.append(_text_page(mcp, doc, token))
    assert _ordinals(pages) == [
        i for i in _CACHE_ORDINALS if i not in _CACHE_RESTRICTED
    ]
    keys = list(record_text_cache._entries)
    assert len(keys) == 2 and old_key in keys
    (new_key,) = [k for k in keys if k != old_key]
    assert new_key.policy_fingerprint != old_key.policy_fingerprint


def test_audit_later_pages_decide_nothing_again_and_record_the_same_trail(
    clean_runtime: str,
    okg_mcp_can_log_in: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Under audit a read's first page filters the record; its later pages run
    no Nomos filter and write no decision row. Without the cache the later
    pages write none either (the trail is insert-once), so the trail is the
    same; the pages are byte-identical."""
    from okg.substrate.mcp import record_text, record_text_cache

    record_text_cache.clear()
    doc = _seed_cache_record(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    _as_reader(monkeypatch, "subject:reader-b")
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    calls = {"n": 0}
    for name in ("filter_rows_by_read_class", "filter_node_rows_for_session"):
        real = getattr(record_text, name)

        def counted(*args, _real=real, **kwargs):
            calls["n"] += 1
            return _real(*args, **kwargs)

        monkeypatch.setattr(record_text, name, counted)

    def decisions() -> int:
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.nomos_decisions")
            return int(cur.fetchone()[0])

    def read(cache_entries: str) -> tuple[list[dict], list[int], list[int]]:
        monkeypatch.setenv("OKG_RECORD_TEXT_CACHE_ENTRIES", cache_entries)
        record_text_cache.clear()
        pages, filters, written, token = [], [], [], None
        for _ in range(50):
            before_calls, before_rows = calls["n"], decisions()
            pages.append(_text_page(mcp, doc, token))
            filters.append(calls["n"] - before_calls)
            written.append(decisions() - before_rows)
            token = pages[-1].get("continuation_token")
            if token is None:
                return pages, filters, written
        raise AssertionError("cursor did not terminate")

    uncached, uncached_filters, uncached_written = read("0")
    cached, cached_filters, cached_written = read("8")
    assert len(cached) > 2, "the fixture must page"
    # Audit hides nothing: every chunk, secret ones included.
    assert _ordinals(cached) == _CACHE_ORDINALS
    assert _page_shape(cached) == _page_shape(uncached)
    assert all(n == 3 for n in uncached_filters), uncached_filters
    assert cached_filters[0] == 3 and set(cached_filters[1:]) == {0}, cached_filters
    # The first read recorded the classes; nothing after adds a row.
    assert uncached_written[0] > 0
    assert set(uncached_written[1:]) == {0}
    assert set(cached_written) == {0}


# --------------------------------------------------------------------------- #
# Issue #2972: a subtype that declares only `text` as searchable -- the shipped
# extraction ontology's DocumentChunk -- stores each chunk's text byte for
# byte. The stamper used to strip ASCII whitespace from the stored value, so a
# chunk no longer matched the source slice its offsets name, and the
# whole-record text read repeated every overlap and lost boundary whitespace.
# --------------------------------------------------------------------------- #

#: The shipped declaration (extraction ontology, DocumentChunk:
#: `okg_searchable_text_fields: text`).
_SHIPPED_TEXT_ONLY = [
    SubtypeSpec("document"),
    SubtypeSpec("document_chunk", metadata={"searchable_text_fields": ["text"]}),
]
_ASCII_WHITESPACE = " \t\n\r\f\v"


@pytest.fixture
def catalog_versions_removed_after(clean_runtime: str):
    """Delete the catalog versions a test applies, so the text-only
    declaration never reaches a later test (`clean_runtime` keeps the
    catalog)."""
    from psycopg import sql

    from tests.substrate.conftest import reset_okg_test_runtime

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        baseline = conn.execute(
            "SELECT coalesce(max(catalog_version_id), 0) FROM okg.catalog_versions"
        ).fetchone()[0]
    yield clean_runtime
    reset_okg_test_runtime(clean_runtime)
    with psycopg.connect(clean_runtime) as conn, conn.transaction():
        for table in (
            "invariants", "identifier_patterns", "aliases", "narrowings",
            "subtype_ancestors", "subtypes", "catalog_versions",
        ):
            conn.execute(
                sql.SQL("DELETE FROM okg.{} WHERE catalog_version_id > %s").format(
                    sql.Identifier(table)
                ),
                (baseline,),
            )


def _chunk_attrs(dsn: str, doc_id: str) -> list[dict]:
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        rows = store.query(
            "SELECT c.attrs FROM okg.graph_nodes c WHERE c.subtype = 'document_chunk' "
            "AND c.node_id IN (SELECT e.src FROM okg.graph_edges e "
            "WHERE e.edge_type = 'member_of' AND e.dst = :doc)",
            {"doc": doc_id},
        )
    finally:
        store.close()
    return [row["attrs"] for row in rows]


def test_text_only_declaration_stores_window_chunks_exactly_and_reads_whole(
    catalog_versions_removed_after: str,
    okg_mcp_can_log_in: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dsn = catalog_versions_removed_after
    text = "\n\n   indented start\n" + "".join(
        f"  row {i:04d}\tvalue\n" for i in range(300)
    ) + "".join(
        f"Log line {i:05d}: the job moved to the next state.\n" for i in range(400)
    ) + "\n\n"
    source_name = "test.text-only-exact"
    _ingest(
        dsn,
        tmp_path,
        monkeypatch,
        files={"ticket.txt": text},
        bundle=ExtractorBundle(
            parsers={"text": TextParser()},
            chunker=None,
            ner=None,
            chunkers_by_format={
                "default": WindowChunker(max_chars=1500, overlap=200)
            },
        ),
        source_name=source_name,
        subtypes=_SHIPPED_TEXT_ONLY,
    )
    doc_id = _doc_id(dsn, source_name, "ticket.txt")

    chunks = _chunk_attrs(dsn, doc_id)
    assert len(chunks) >= 15
    edged = 0
    for attrs in chunks:
        want = text[int(attrs["char_offset"]):int(attrs["char_end"])]
        assert attrs["text"] == want, (attrs["char_offset"], attrs["text"][-20:])
        edged += want != want.strip(_ASCII_WHITESPACE)
    # Not vacuous: chunks whose source slice starts or ends in whitespace are
    # the ones the old strip rewrote.
    assert edged >= 5, edged

    mcp = build_server(dsn=dsn, deployment_name="default")
    try:
        whole = _read_text(mcp, doc_id)
    finally:
        mcp._okg_pool.close()  # type: ignore[attr-defined]
    items = [item for page in whole for item in page["items"]]
    assert "".join(_unfenced(item) for item in items) == text
    assert all(item.get("trimmed_chars") == 200 for item in items[1:])


def test_text_only_declaration_keeps_markdown_chunk_whitespace(
    catalog_versions_removed_after: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dsn = catalog_versions_removed_after
    text = "".join(
        f"# Section {s}\n\n"
        + "".join(f"Paragraph {s}.{p} " + "word " * 60 + "\n\n" for p in range(4))
        for s in range(4)
    )
    chunker = MarkdownHeadingChunker(min_tokens=100, max_tokens=1500)
    expected = sorted(chunk.text for chunk in chunker.chunk(text))
    # Not vacuous: the heading chunker's sections end in whitespace.
    assert all(t != t.strip(_ASCII_WHITESPACE) for t in expected)
    source_name = "test.text-only-markdown"
    _ingest(
        dsn,
        tmp_path,
        monkeypatch,
        files={"notes.md": text},
        bundle=ExtractorBundle(
            parsers={"markdown": MarkdownParser()},
            chunker=None,
            ner=None,
            chunkers_by_format={"markdown": chunker},
        ),
        source_name=source_name,
        subtypes=_SHIPPED_TEXT_ONLY,
    )
    doc_id = _doc_id(dsn, source_name, "notes.md")
    stored = sorted(attrs["text"] for attrs in _chunk_attrs(dsn, doc_id))
    assert stored == expected


def test_heading_chunked_markdown_stores_exact_spans_and_reads_whole(
    catalog_versions_removed_after: str,
    okg_mcp_can_log_in: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #2997: a whole read of heading-chunked Markdown is the file.

    Before the fix the chunks carried no span, the source stored offset 0 on
    each, and the read joined them without the blank line at every section
    seam while it reported `complete`."""
    dsn = catalog_versions_removed_after
    text = "Intro paragraph before any heading.\n\n" + "".join(
        f"## Section {s}: the valve\n\n"
        + "".join(
            f"Paragraph {s}.{p} " + "the pump feeds the valve; " * 30 + "\n\n"
            for p in range(3)
        )
        for s in range(8)
    ) + "Last line.\n"
    source_name = "test.markdown-heading-exact"
    _ingest(
        dsn,
        tmp_path,
        monkeypatch,
        files={"notes.md": text},
        bundle=ExtractorBundle(
            parsers={"markdown": MarkdownParser()},
            chunker=None,
            ner=None,
            chunkers_by_format={
                "markdown": MarkdownHeadingChunker(min_tokens=100, max_tokens=1500)
            },
        ),
        source_name=source_name,
        subtypes=_SHIPPED_TEXT_ONLY,
    )
    doc_id = _doc_id(dsn, source_name, "notes.md")

    chunks = _chunk_attrs(dsn, doc_id)
    assert len(chunks) >= 8
    for attrs in chunks:
        assert attrs["text"] == text[int(attrs["char_offset"]):int(attrs["char_end"])]
    spans = sorted((int(a["char_offset"]), int(a["char_end"])) for a in chunks)
    assert spans[0][0] == 0 and spans[-1][1] == len(text)
    assert all(a[1] == b[0] for a, b in zip(spans, spans[1:])), spans

    mcp = build_server(dsn=dsn, deployment_name="default")
    try:
        whole = _read_text(mcp, doc_id)
    finally:
        mcp._okg_pool.close()  # type: ignore[attr-defined]
    assert whole[-1]["coverage"] == "complete"
    items = [item for page in whole for item in page["items"]]
    assert "".join(_unfenced(item) for item in items) == text
    assert not any(item.get("trimmed_chars") for item in items)


class _PagedParser:
    """Stands in for PdfTextParser: pages joined by a blank line, each page
    chunk carrying its span in that text (pypdf needs a real PDF; the parser's
    own spans are pinned in test_parser_spans.py)."""

    def parse(self, content: bytes | str, **_: object) -> ParsedDocument:
        text = content.decode() if isinstance(content, bytes) else content
        pages = text.split("\f")
        chunks, position = [], 0
        for i, page in enumerate(pages):
            if i:
                position += 2
            chunks.append(ParsedChunk(page, heading_path=[f"page {i + 1}"],
                                      span=(position, position + len(page))))
            position += len(page)
        return ParsedDocument(text="\n\n".join(pages), chunks=chunks)


class _SpanlessParser:
    """Stands in for HtmlLxmlParser: paragraph chunks with no span, and a
    heading that is in the text but in no chunk."""

    def parse(self, content: bytes | str, **_: object) -> ParsedDocument:
        text = content.decode() if isinstance(content, bytes) else content
        lines = [line for line in text.splitlines() if line]
        return ParsedDocument(
            text=" ".join(lines),
            chunks=[ParsedChunk(line) for line in lines[1:]],
        )


def test_paged_text_stores_real_spans_and_separators_and_reads_exactly(
    clean_runtime: str,
    okg_mcp_can_log_in: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A whole read of a paged document is the parser's text byte for byte
    (page separators included) and says complete; a windowed page's pieces
    carry document offsets."""
    pages = [
        "Page one ends with https://example.org/talk",
        "",
        "Title of page three\n" + "Body sentence on page three. " * 80,
        "Last page.\n",
    ]
    source_name = "test.paged-exact"
    _ingest(
        clean_runtime,
        tmp_path,
        monkeypatch,
        files={"paper.txt": "\f".join(pages)},
        bundle=ExtractorBundle(
            parsers={"text": _PagedParser()},
            chunker=None,
            ner=None,
            chunkers_by_format={
                "default": WindowChunker(max_chars=1500, overlap=200)
            },
        ),
        source_name=source_name,
    )
    doc_id = _doc_id(clean_runtime, source_name, "paper.txt")
    text = "\n\n".join(pages)

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        edges = store.query(
            "SELECT e.attrs, c.attrs->>'text' AS text FROM okg.graph_edges e "
            "JOIN okg.graph_nodes c ON c.node_id = e.src "
            "WHERE e.edge_type = 'member_of' AND e.dst = :doc",
            {"doc": doc_id},
        )
    finally:
        store.close()
    assert len(edges) >= 5
    for row in edges:
        attrs = row["attrs"]
        start, end = int(attrs["char_offset"]), int(attrs["char_end"])
        assert text[start:end] == row["text"]
        assert int(attrs["doc_char_length"]) == len(text)
    separators = sorted(
        row["attrs"]["separator_before"] for row in edges
        if "separator_before" in row["attrs"]
    )
    # Page 3 follows the empty page 2: its separator spans both joins.
    assert separators == ["\n\n", "\n\n\n\n"]

    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    whole = _read_text(mcp, doc_id)
    items = [item for page in whole for item in page["items"]]
    # The empty page is served as an empty, unfenced segment.
    assert [i["text"] for i in items].count("") == 1
    assert "".join(_unfenced(i) for i in items if i["text"]) == text
    assert {page["coverage"] for page in whole} == {"complete"}
    assert sorted(i.get("separator_before_chars", 0) for i in items)[-2:] == [2, 4]


def test_spanless_chunks_store_no_position_and_never_read_complete(
    clean_runtime: str,
    okg_mcp_can_log_in: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every chunk of an HTML-like page arrives, yet its heading is in no
    chunk: with no positions the read says unknown, never complete."""
    source_name = "test.spanless"
    _ingest(
        clean_runtime,
        tmp_path,
        monkeypatch,
        files={"page.txt": "Heading nobody chunked\nFirst paragraph.\nSecond one.\n"},
        bundle=ExtractorBundle(
            parsers={"text": _SpanlessParser()}, chunker=None, ner=None
        ),
        source_name=source_name,
    )
    doc_id = _doc_id(clean_runtime, source_name, "page.txt")
    for attrs in _chunk_attrs(clean_runtime, doc_id):
        assert "char_offset" not in attrs and "char_end" not in attrs, attrs
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    (page,) = _read_text(mcp, doc_id)
    assert [_unfenced(i) for i in page["items"]] == ["First paragraph.", "Second one."]
    assert page["order_basis"] == "chunk_ordinal"
    assert page["coverage"] == "unknown"


def test_guided_markdown_whole_read_coverage(
    clean_runtime: str,
    okg_mcp_can_log_in: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The guided default for Markdown (MarkdownParser + MarkdownHeadingChunker).

    #2997 (PR #3004) gave its heading chunks their real spans; with the
    positions, `doc_char_length` and separators this change stores, the whole
    read is the file byte for byte and says `complete` (without #3004 the
    chunks stored no position and the read said `unknown`)."""
    text = "Intro paragraph before any heading.\n\n" + "".join(
        f"## Section {s}: the valve\n\n"
        + "".join(
            f"Paragraph {s}.{p} " + "the pump feeds the valve; " * 30 + "\n\n"
            for p in range(3)
        )
        for s in range(6)
    ) + "Last line.\n"
    source_name = "test.guided-markdown-coverage"
    _ingest(
        clean_runtime,
        tmp_path,
        monkeypatch,
        files={"notes.md": text},
        bundle=ExtractorBundle(
            parsers={"markdown": MarkdownParser()},
            chunker=None,
            ner=None,
            chunkers_by_format={
                "markdown": MarkdownHeadingChunker(min_tokens=100, max_tokens=1500)
            },
        ),
        source_name=source_name,
    )
    doc_id = _doc_id(clean_runtime, source_name, "notes.md")
    chunks = _chunk_attrs(clean_runtime, doc_id)
    assert len(chunks) >= 6
    for attrs in chunks:
        start, end = int(attrs["char_offset"]), int(attrs["char_end"])
        assert text[start:end] == attrs["text"]

    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    whole = _read_text(mcp, doc_id)
    items = [item for page in whole for item in page["items"]]
    assert "".join(_unfenced(item) for item in items) == text
    assert {page["coverage"] for page in whole} == {"complete"}
