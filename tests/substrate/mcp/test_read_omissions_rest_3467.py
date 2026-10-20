"""Reads say what they left out with Nomos on, in windows and in lists
(issue #3467, PACT read-omissions-rest-3467).

PACT read-omissions-3467 made a record read count its passages and an
outgoing `expand` count its incoming links, but only with Nomos off; every
deployment that runs Nomos in audit (CMS among them) got the old silent
"complete" read. Record-scoped grep windows dropped a named `doc:` or
`file:` record outside the default search profile and answered "0 matches,
exhaustive" (`profile` is refused there, so nothing could read it), and a
`file:` record's windows searched only its path. A `view="list"` row for a
chunk named only the chunk.

With a Nomos runtime the two notices are read from the catalog and the call
alone: the same words whether the passages or links exist, are readable or
are hidden, so they disclose nothing (#3559's rule: what a reader may not
read never shapes the answer).
"""
from __future__ import annotations

import json
import re
import uuid

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import open_session
from okg.substrate.mcp.result_text import render
from okg.substrate.mcp.search_views import list_projection
from okg.substrate.mcp.tools import expand_neighbors, inspect_node
from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph
from tests.substrate.mcp.test_read_omissions_3467 import _seed

pytestmark = pytest.mark.postgres


@pytest.fixture
def session_conn(clean_runtime):
    _seed(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        yield conn, open_session(conn, deployment_name="default")


def _nomos_on(monkeypatch, on: bool) -> list:
    """Run the reads as with (or without) a Nomos runtime; return the
    bounded counting reads they make."""
    import okg.substrate.mcp.tools as tools_module

    reads: list = []
    real = tools_module._visible_edge_rows
    monkeypatch.setattr(tools_module, "_visible_edge_rows",
                        lambda *a, **k: reads.append(k) or real(*a, **k))
    monkeypatch.setattr(tools_module, "deployment_requires_nomos_runtime",
                        lambda _name: on)
    return reads


# ── 1 and 5: the notices under a Nomos runtime ─────────────────────────────


def test_under_nomos_a_record_read_still_names_its_passage_read(session_conn, monkeypatch):
    """Before: under audit the ticket/document read said nothing about its
    passages. After: it names the read, with no count, and says the same
    for a document with two chunks and one with none."""
    conn, s = session_conn
    counting = _nomos_on(monkeypatch, True)
    with_chunks = inspect_node(conn, s, "doc:r:reaper.yaml")["passages_not_shown"]
    without = inspect_node(conn, s, "doc:r:empty.md")["passages_not_shown"]
    assert with_chunks == {
        "count": None, "count_basis": "not_counted", "edge_type": "contains",
        "child_subtype": "document_chunk",
        "read": 'inspect(target={"kind":"text","node_id":"doc:r:reaper.yaml"})',
    }
    assert {k: v for k, v in without.items() if k != "read"} == {
        k: v for k, v in with_chunks.items() if k != "read"}
    assert not [k for k in counting if k.get("limit") is not None]
    text = render(dict(inspect_node(conn, s, "doc:r:reaper.yaml"), operator_name="inspect"),
                  tool="inspect", kind="node")
    assert ("! this read leaves out document_chunk passages, if any; read them with "
            'inspect(target={"kind":"text","node_id":"doc:r:reaper.yaml"})') in text
    # A subtype that declares no passages still says nothing.
    assert "passages_not_shown" not in inspect_node(conn, s, "ticket:A-1")


def test_under_nomos_an_outgoing_expand_says_incoming_links_are_not_read(
    session_conn, monkeypatch,
):
    """Before: under audit the g075 ticket's default expand hid its two
    backlinks with no word. After: every first outgoing page says incoming
    links are not read, uncounted, alike for a node with two and with none."""
    conn, s = session_conn
    counting = _nomos_on(monkeypatch, True)
    linked = expand_neighbors(conn, s, "ticket:A-1")
    unlinked = expand_neighbors(conn, s, "ticket:B-2")
    notice = {"count": None, "count_basis": "not_counted", "direction": "in"}
    assert linked["incoming_not_shown"] == notice == unlinked["incoming_not_shown"]
    assert not [k for k in counting if k.get("limit") is not None]
    text = render(dict(linked, operator_name="expand"), tool="expand")
    assert '! incoming links, if any, are not shown; read them with direction="in"' in text
    assert "incoming_not_shown" not in expand_neighbors(conn, s, "ticket:A-1", direction="in")
    assert "incoming_not_shown" not in expand_neighbors(conn, s, "ticket:A-1", offset=1)


def test_with_nomos_off_the_counted_notices_are_unchanged(session_conn, monkeypatch):
    """Control: no runtime keeps PACT read-omissions-3467's counts."""
    conn, s = session_conn
    _nomos_on(monkeypatch, False)
    assert expand_neighbors(conn, s, "ticket:A-1")["incoming_not_shown"]["count"] == 2
    assert "incoming_not_shown" not in expand_neighbors(conn, s, "ticket:B-2")
    assert inspect_node(conn, s, "doc:r:reaper.yaml")["passages_not_shown"]["count"] == 2
    assert "passages_not_shown" not in inspect_node(conn, s, "doc:r:empty.md")


# ── 4: record windows on doc: and file: records ─────────────────────────────


def _policy() -> dict:
    return {
        "policy_bundle_id": f"nomos:test:g3:{uuid.uuid4().hex[:8]}",
        "policy_version": "v1",
        "default_effect": "allow",
        "rules": [
            {"rule_id": "hide_secret", "effect": "deny", "priority": 300,
             "subjects": ["subject:mcp-reader"], "actions": ["graph.read.mcp"],
             "match": {"classification": "secret"}},
            {"rule_id": "allow_read", "effect": "allow", "priority": 100,
             "subjects": ["subject:mcp-reader"], "actions": ["graph.read.mcp"]},
        ],
    }


def _classification(node_id: str) -> str:
    return "secret" if node_id == "doc:r:secret.py" else "internal"


def _classified_nodes(conn, nodes: list[tuple[str, str, str]]) -> None:
    ChronosGraphStore.from_connection(conn).apply_facts([
        NodeFact(node_id=nid, subtype=subtype,
                 attrs={"text": text, "classification": _classification(nid)},
                 source_record_id={"id": nid, "source": "fixture"}, source_revision={"v": 1})
        for nid, subtype, text in nodes
    ], source="default")


def _classified_edges(conn, edges: list[tuple[str, str, str]]) -> None:
    ChronosGraphStore.from_connection(conn).apply_facts([
        EdgeFact(src=src, dst=dst, edge_type=etype,
                 attrs={"classification": max(_classification(src), _classification(dst))},
                 source_record_id={"src": src, "dst": dst, "source": "fixture"},
                 source_revision={"v": 1})
        for src, dst, etype in edges
    ], source="default")


@pytest.fixture
def served(clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    """The omission graph plus a hidden document derived from a file, served
    by a deployment whose default search profile holds tickets only (the CMS
    `content` profile likewise leaves out `document`)."""
    from okg.substrate.mcp.server import build_server
    from tests.substrate.mcp import test_read_omissions_3467 as omissions
    from tests.substrate.test_get_node_missing import _publish

    # The policy is active before the first publish, and every row carries a
    # classification, as the windows e2e fixture's rows do: under enforce an
    # unclassified row is read by no one.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        activate_policy_bundle(
            conn, deployment_name="default", bundle=compile_policy_graph(_policy()),
            scope="deployment:default", activated_by="test:operator", bootstrap=True,
        )
    monkeypatch.setattr(omissions, "_insert_node_facts", _classified_nodes)
    monkeypatch.setattr(omissions, "_insert_edge_facts", _classified_edges)
    _seed(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _classified_nodes(conn, [
            ("file:r:secret.py", "source_file", "secret.py"),
            ("doc:r:secret.py", "document", "secret.py"),
            ("chunk:s", "document_chunk", "reaperCount: 3"),
        ])
        _classified_edges(conn, [
            ("doc:r:secret.py", "file:r:secret.py", "derived_from"),
            ("doc:r:secret.py", "chunk:s", "contains"),
        ])
    _publish(clean_runtime)
    directory = tmp_path / "default"
    (directory / "schemas").mkdir(parents=True)
    (directory / "source_registry.yaml").write_text("sources: {}\n", encoding="utf-8")
    (directory / "deployment.yaml").write_text(json.dumps({
        "name": "default", "description": "issue 3467 fixture", "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "search": {"default_profile": "tickets",
                   "profiles": {"tickets": {"subtypes": ["ticket"]},
                                "content": {"subtypes": ["ticket", "document_chunk"]}}},
    }), encoding="utf-8")
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        yield mcp._tool_manager._tools["search"].fn
    finally:
        mcp._okg_pool.close()


def _windows(search, records: list[str], query: str = "reaperCount") -> dict:
    return search(query=query, method="grep", view="windows",
                  scope={"records": records}, max_bytes=65536)


def test_windows_read_a_named_document_outside_the_default_profile(served):
    """Before: 0 records, coverage exhaustive, the document listed only in
    `records_outside_profile`, and `profile` refused. After: the named
    document is read and its chunk matches."""
    page = _windows(served, ["doc:r:reaper.yaml"])
    assert page["total_count"] == 1
    assert [row["node_id"] for row in page["results"]] == ["doc:r:reaper.yaml"]
    assert "records_outside_profile" not in page
    assert "search_profile" not in page and "scoped_subtypes" not in page


def test_windows_on_a_file_name_the_document_that_holds_its_content(served):
    """Before: a `file:` record's windows searched only its path and said
    nothing more. After: the page names the document derived from it."""
    page = _windows(served, ["file:r:reaper.yaml", "symbol:r:reaper.yaml:main"])
    assert page["total_count"] == 0
    assert page["text_elsewhere"] == [
        {"record_id": "file:r:reaper.yaml", "document_id": "doc:r:reaper.yaml"},
        {"record_id": "symbol:r:reaper.yaml:main", "document_id": "doc:r:reaper.yaml"},
    ]
    text = render(dict(_windows(served, ["file:r:reaper.yaml"]), operator_name="search"),
                  tool="search")
    assert ('! the content of "file:r:reaper.yaml" is in "doc:r:reaper.yaml"; search it '
            'with scope={"records":["doc:r:reaper.yaml"]}') in text
    # A file with no document, or one whose document has no chunks: no pointer.
    assert "text_elsewhere" not in _windows(served, ["file:r:lonely.py", "file:r:empty.md"])
    # A document read whole is never pointed elsewhere.
    assert "text_elsewhere" not in _windows(served, ["doc:r:reaper.yaml"])


def test_under_enforce_a_hidden_document_is_never_named(served, monkeypatch):
    """The document derived from `file:r:secret.py` is hidden: under enforce
    the file's page is the page of a file with no document, key for key."""
    assert _windows(served, ["file:r:secret.py"])["text_elsewhere"] == [
        {"record_id": "file:r:secret.py", "document_id": "doc:r:secret.py"}]
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-reader")
    # Control: a readable document is still named under enforce.
    assert _windows(served, ["file:r:reaper.yaml"])["text_elsewhere"] == [
        {"record_id": "file:r:reaper.yaml", "document_id": "doc:r:reaper.yaml"}]
    hidden = _windows(served, ["file:r:secret.py"])
    absent = _windows(served, ["file:r:lonely.py"])
    assert "text_elsewhere" not in hidden
    assert set(hidden) == set(absent)
    for key in ("total_count", "total_count_basis", "window_count", "match_count", "coverage"):
        assert hidden[key] == absent[key], key
    assert "secret" not in json.dumps(hidden).replace("file:r:secret.py", "")


# ── comment: a list row names its parent ────────────────────────────────────


def test_a_listed_passage_row_names_its_parent():
    """No database: the list projection keeps a resolved parent's id (the one
    the card prints) and the text line says `in <id>`; an unresolved parent
    and a record row add nothing."""
    page = {"view": "cards", "results": [
        {"node_id": "chunk:1", "subtype": "document_chunk", "unit": "passage",
         "parent": {"state": "resolved", "node_id": "doc:r:reaper.yaml",
                    "subtype": "document", "title": "reaper.yaml"}},
        {"node_id": "chunk:9", "subtype": "document_chunk", "unit": "passage",
         "parent": {"state": "declared_but_unlinked", "node_id": None}},
        {"node_id": "ticket:A-1", "subtype": "ticket", "parent": None},
    ]}
    listed = list_projection(page)
    assert [row.get("parent") for row in listed["results"]] == [
        {"node_id": "doc:r:reaper.yaml"}, None, None]
    text = render(dict(listed, operator_name="search", query="reaperCount"), tool="search")
    (line,) = [ln for ln in text.splitlines() if "chunk:1" in ln]
    assert "in doc:r:reaper.yaml" in line
    assert "reaper.yaml" not in text.replace("doc:r:reaper.yaml", "")


def test_a_list_search_names_each_chunk_rows_document(served):
    """The comment on #3467: a profile that holds chunks but not documents
    (CMS `content`) lists chunk rows. Before: each row named only the chunk.
    After: each names its document."""
    page = served(query="reaperCount", method="lexical", view="list", profile="content")
    rows = {row["node_id"]: row for row in page["results"]}
    assert rows["chunk:1"]["parent"] == {"node_id": "doc:r:reaper.yaml"}
    text = render(dict(page, operator_name="search"), tool="search")
    (line,) = [ln for ln in text.splitlines() if re.match(r"\[\d+\] chunk:1 ", ln)]
    # read-passage-rows-3666 (D1): a list row prints its head line, whose
    # document field names the chunk's document (fenced on an untrusted row).
    assert '"doc:r:reaper.yaml"' in line or "in doc:r:reaper.yaml" in line, line
    # The hidden document's chunk is readable here (Nomos off); its row
    # names the document the card names.
    assert rows["chunk:s"]["parent"] == {"node_id": "doc:r:secret.py"}


def _enforce(monkeypatch) -> None:
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-reader")


def test_under_enforce_the_notices_read_alike_for_hidden_and_absent(
    served, clean_runtime, monkeypatch,
):
    """A real enforce runtime, not a simulated one: `file:r:secret.py`'s only
    incoming link comes from a hidden document, `file:r:lonely.py` has none;
    a document with readable chunks and one with none. Each pair gets the
    same notice."""
    _enforce(monkeypatch)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default", nomos_subject="subject:mcp-reader")
        hidden_link = expand_neighbors(conn, s, "file:r:secret.py")
        no_link = expand_neighbors(conn, s, "file:r:lonely.py")
        assert hidden_link["incoming_not_shown"] == no_link["incoming_not_shown"] == {
            "count": None, "count_basis": "not_counted", "direction": "in"}
        with_chunks = inspect_node(conn, s, "doc:r:reaper.yaml")["passages_not_shown"]
        without = inspect_node(conn, s, "doc:r:empty.md")["passages_not_shown"]
    assert {k: v for k, v in with_chunks.items() if k != "read"} == {
        k: v for k, v in without.items() if k != "read"}
    assert with_chunks["count"] is None


def test_under_enforce_a_listed_chunk_never_names_a_hidden_parent(served, monkeypatch):
    """`chunk:s` is readable but its document is hidden: under enforce its
    list row names no parent, while `chunk:1` still names its document."""
    _enforce(monkeypatch)
    page = served(query="reaperCount", method="lexical", view="list", profile="content")
    rows = {row["node_id"]: row for row in page["results"]}
    assert rows["chunk:1"]["parent"] == {"node_id": "doc:r:reaper.yaml"}
    assert "chunk:s" in rows and "parent" not in rows["chunk:s"]
    assert "secret.py" not in json.dumps(page)
    assert page["parent_relation"] == {}
