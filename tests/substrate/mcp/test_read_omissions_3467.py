"""Reads say what they left out and where it is (issue #3467, PACT
read-omissions-3467).

From the C4 comp-ops audit: a ticket read said "complete" and never mentioned
its comments; a default outgoing `expand` never said that incoming links
exist (the g075 answer sat on one); and a text read of a code symbol or file
ended at `no_text_chunks` while its text sat in the document derived from the
file. Each read keeps its rows and adds one pointer: how many passages and
which read returns them, how many incoming links and the direction that reads
them, and the document whose text to read.
"""
from __future__ import annotations

import json

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import open_session
from okg.substrate.mcp.record_text import prepare_record_text, read_record_text
from okg.substrate.mcp.result_text import render
from okg.substrate.mcp.tools import expand_neighbors, inspect_node
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.substrate.test_get_node_missing import (
    _insert_edge_facts,
    _insert_node_facts,
    _publish,
)


def _seed(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["source_file", "document", "document_chunk", "visual_artifact",
                      "code_symbol", "ticket", "commit", "diff"],
            narrowings=[
                ("document", "derived_from", "source_file"),
                ("document", "contains", "document_chunk"),
                ("document", "contains", "visual_artifact"),
                ("source_file", "contains", "code_symbol"),
                ("ticket", "references", "ticket"),
                ("commit", "contains", "diff"),
            ],
        )
        for parent, child in (("document", "document_chunk"), ("commit", "diff")):
            conn.execute(
                "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb) || "
                "jsonb_build_object('passage_rollup', jsonb_build_object("
                "'edge_type', 'contains', 'child_subtype', %s::text)) WHERE name = %s",
                (child, parent),
            )
        _insert_node_facts(conn, [
            ("file:r:reaper.yaml", "source_file", "reaper.yaml"),
            ("file:r:lonely.py", "source_file", "lonely.py"),
            ("file:r:empty.md", "source_file", "empty.md"),
            ("doc:r:reaper.yaml", "document", "reaper.yaml"),
            ("doc:r:empty.md", "document", "empty.md"),
            ("chunk:1", "document_chunk", "reaperCount: 8"),
            ("chunk:2", "document_chunk", "chunkSize: 4000"),
            ("figure:1", "visual_artifact", "a figure"),
            ("symbol:r:reaper.yaml:main", "code_symbol", ""),
            ("symbol:r:lonely.py:f", "code_symbol", ""),
            ("symbol:r:empty.md:h", "code_symbol", ""),
            ("ticket:A-1", "ticket", "the dataset"),
            ("ticket:B-2", "ticket", "invalidated A-1 on 2026-03-09"),
            ("ticket:C-3", "ticket", "see A-1"),
            ("commit:r:abc", "commit", "fix reaper"),
            ("diff:r:abc:1", "diff", "- 4\n+ 8"),
        ])
        _insert_edge_facts(conn, [
            ("doc:r:reaper.yaml", "file:r:reaper.yaml", "derived_from"),
            ("doc:r:reaper.yaml", "chunk:1", "contains"),
            ("doc:r:reaper.yaml", "chunk:2", "contains"),
            ("doc:r:reaper.yaml", "figure:1", "contains"),
            ("doc:r:empty.md", "file:r:empty.md", "derived_from"),
            ("file:r:reaper.yaml", "symbol:r:reaper.yaml:main", "contains"),
            ("file:r:lonely.py", "symbol:r:lonely.py:f", "contains"),
            ("file:r:empty.md", "symbol:r:empty.md:h", "contains"),
            ("ticket:B-2", "ticket:A-1", "references"),
            ("ticket:C-3", "ticket:A-1", "references"),
            ("ticket:A-1", "ticket:A-1", "references"),
            ("commit:r:abc", "diff:r:abc:1", "contains"),
        ])
        _insert_spanned_symbols(conn)
    _publish(dsn)


#: Symbols of `file:r:reaper.yaml` by the span they store: (start_line, end_line).
_SPANS = {
    "symbol:r:reaper.yaml:spanned": {"start_line": 2, "end_line": 2},
    "symbol:r:reaper.yaml:one_line": {"start_line": 1, "end_line": 1},
    "symbol:r:reaper.yaml:no_span": {},
    "symbol:r:reaper.yaml:backwards": {"start_line": 5, "end_line": 3},
    "symbol:r:reaper.yaml:text_span": {"start_line": "2", "end_line": "2"},
    "symbol:r:reaper.yaml:bool_span": {"start_line": True, "end_line": True},
    "symbol:r:reaper.yaml:zero_span": {"start_line": 0, "end_line": 2},
}


def _insert_spanned_symbols(conn: psycopg.Connection) -> None:
    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts([
        NodeFact(
            node_id=node_id, subtype="code_symbol", attrs={"text": "", **attrs},
            source_record_id={"id": node_id, "source": "fixture"},
            source_revision={"v": 1},
        )
        for node_id, attrs in _SPANS.items()
    ])
    store.apply_facts([
        EdgeFact(
            src="file:r:reaper.yaml", dst=node_id, edge_type="contains", attrs={},
            source_record_id={"src": "file:r:reaper.yaml", "dst": node_id,
                              "source": "fixture"},
            source_revision={"v": 1},
        )
        for node_id in _SPANS
    ])


@pytest.fixture
def session_conn(clean_runtime):
    _seed(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        yield conn, open_session(conn, deployment_name="default")


@pytest.mark.postgres
def test_a_record_read_counts_its_passages_and_names_the_read(session_conn):
    conn, s = session_conn
    doc = inspect_node(conn, s, "doc:r:reaper.yaml")
    # Two chunks; the figure the document also contains is not a passage.
    assert doc["passages_not_shown"] == {
        "count": 2, "count_basis": "exact", "edge_type": "contains",
        "child_subtype": "document_chunk",
        "read": 'inspect(target={"kind":"text","node_id":"doc:r:reaper.yaml"})',
    }
    text = render(dict(doc, operator_name="inspect"), tool="inspect", kind="node")
    assert ("2 document_chunk passages are not in this read; read them with "
            'inspect(target={"kind":"text","node_id":"doc:r:reaper.yaml"})') in text
    # A subtype that declares no passages gets no key, nor a document with none.
    assert "passages_not_shown" not in inspect_node(conn, s, "ticket:A-1")
    assert "passages_not_shown" not in inspect_node(conn, s, "doc:r:empty.md")


@pytest.mark.postgres
def test_non_chunk_passages_point_at_expand_not_the_text_read(session_conn):
    """The text read gathers document chunks only; a commit's diffs are read
    by expanding its declared edge."""
    conn, s = session_conn
    commit = inspect_node(conn, s, "commit:r:abc")
    assert commit["passages_not_shown"]["read"] == (
        'expand(input={"node_id":"commit:r:abc"}, edge_type="contains", direction="out")')


@pytest.mark.postgres
def test_an_outgoing_expand_says_how_many_incoming_links_it_left_out(session_conn):
    conn, s = session_conn
    page = expand_neighbors(conn, s, "ticket:A-1")
    # The self-loop is on the outgoing page already; it is not counted in.
    assert [n["dst"] for n in page["neighbors"]] == ["ticket:A-1"]
    assert page["incoming_not_shown"] == {
        "count": 2, "count_basis": "exact", "direction": "in"}
    text = render(dict(page, operator_name="expand"), tool="expand")
    assert '! 2 incoming links not shown; read them with direction="in"' in text
    narrowed = expand_neighbors(conn, s, "ticket:A-1", edge_type="contains")
    assert "incoming_not_shown" not in narrowed
    assert "incoming_not_shown" not in expand_neighbors(conn, s, "ticket:A-1",
                                                        direction="in")
    assert "incoming_not_shown" not in expand_neighbors(conn, s, "ticket:B-2")


@pytest.mark.postgres
def test_with_a_nomos_runtime_the_reads_count_no_unread_rows(
    session_conn, monkeypatch,
):
    """Under audit or enforce the authorized edge read records a decision
    for every counted edge the reader never receives (and under enforce the
    count would reflect hidden rows), so no omission count is read; the
    notices stay, uncounted (PACT read-omissions-rest-3467)."""
    import okg.substrate.mcp.tools as tools_module

    conn, s = session_conn
    reads: list = []
    real = tools_module._visible_edge_rows
    monkeypatch.setattr(tools_module, "_visible_edge_rows",
                        lambda *a, **k: reads.append(k) or real(*a, **k))
    monkeypatch.setattr(tools_module, "deployment_requires_nomos_runtime",
                        lambda _name: True)
    assert inspect_node(conn, s, "doc:r:reaper.yaml")["passages_not_shown"]["count"] is None
    assert expand_neighbors(conn, s, "ticket:A-1")["incoming_not_shown"]["count"] is None
    assert not [k for k in reads if k.get("limit") is not None]
    # Control: with no runtime the same reads count through that read.
    monkeypatch.setattr(tools_module, "deployment_requires_nomos_runtime",
                        lambda _name: False)
    assert "incoming_not_shown" in expand_neighbors(conn, s, "ticket:A-1")
    assert [k for k in reads if k.get("limit") is not None]


@pytest.mark.postgres
@pytest.mark.parametrize("node_id", ["symbol:r:reaper.yaml:main", "file:r:reaper.yaml"])
def test_a_code_node_without_text_points_at_its_document(session_conn, node_id):
    conn, s = session_conn
    outcome = prepare_record_text(conn, s, node_id).payload["outcome"]
    assert outcome["code"] == "no_text_chunks"
    assert outcome["next_move"] == (
        "its text is in the document derived from its file; read it with "
        'inspect(target={"kind":"text","node_id":"doc:r:reaper.yaml"})')


_DOC_READ = 'inspect(target={"kind":"text","node_id":"doc:r:reaper.yaml"})'


@pytest.mark.postgres
def test_a_symbol_with_a_stored_span_points_at_just_its_lines(session_conn):
    """Issue #3982: the suggested read names the symbol's own lines, so the
    agent reads that definition and not the whole file."""
    conn, s = session_conn
    outcome = prepare_record_text(
        conn, s, "symbol:r:reaper.yaml:spanned").payload["outcome"]
    assert outcome["code"] == "no_text_chunks"
    assert outcome["next_move"] == (
        "its text is in the document derived from its file; read it with "
        'inspect(target={"kind":"text","node_id":"doc:r:reaper.yaml",'
        '"lines":[[2,2]]})')
    # A one-line symbol keeps its one-line range.
    one = prepare_record_text(
        conn, s, "symbol:r:reaper.yaml:one_line").payload["outcome"]
    assert one["next_move"].endswith('"lines":[[1,1]]})')


@pytest.mark.postgres
def test_the_suggested_read_is_servable_as_given(session_conn):
    """The move is a call the read tool accepts: its target parses, and
    served with that target it returns the symbol's lines and no others."""
    conn, s = session_conn
    move = prepare_record_text(
        conn, s, "symbol:r:reaper.yaml:spanned").payload["outcome"]["next_move"]
    prefix, suffix = "read it with inspect(target=", ")"
    assert move.endswith(suffix)
    target = json.loads(move[move.index(prefix) + len(prefix):-len(suffix)])
    assert target == {"kind": "text", "node_id": "doc:r:reaper.yaml",
                      "lines": [[2, 2]]}
    page = read_record_text(conn, s, target["node_id"], lines=target["lines"])
    assert page["status"] == "ok"
    assert page["lines"]["requested"] == [[2, 2]]
    served = "".join(item["text"] for item in page["items"])
    assert "chunkSize: 4000" in served
    assert "reaperCount: 8" not in served
    # The whole-document read the old pointer named returns both lines.
    whole = read_record_text(conn, s, "doc:r:reaper.yaml")
    whole_text = "".join(item["text"] for item in whole["items"])
    assert "reaperCount: 8" in whole_text and "chunkSize: 4000" in whole_text


@pytest.mark.postgres
@pytest.mark.parametrize("node_id", [
    "symbol:r:reaper.yaml:main",       # no span attrs at all
    "symbol:r:reaper.yaml:no_span",    # attrs present, no span
    "symbol:r:reaper.yaml:backwards",  # start after end
    "symbol:r:reaper.yaml:text_span",  # not whole numbers
    "symbol:r:reaper.yaml:bool_span",  # a bool is not a line number
    "symbol:r:reaper.yaml:zero_span",  # lines count from 1
    "file:r:reaper.yaml",              # a file has no span
])
def test_a_node_with_no_usable_span_keeps_the_whole_document_pointer(
    session_conn, node_id,
):
    conn, s = session_conn
    outcome = prepare_record_text(conn, s, node_id).payload["outcome"]
    assert outcome["next_move"] == (
        "its text is in the document derived from its file; read it with "
        + _DOC_READ)


@pytest.mark.postgres
@pytest.mark.parametrize("node_id", ["symbol:r:lonely.py:f", "symbol:r:empty.md:h"])
def test_a_code_node_with_no_readable_document_keeps_the_old_pointer(session_conn, node_id):
    """No derived document, or one with no chunks: never point at a read
    that would come back empty."""
    conn, s = session_conn
    outcome = prepare_record_text(conn, s, node_id).payload["outcome"]
    assert outcome["next_move"].startswith('inspect(target={"kind":"node"')


def test_a_search_page_selecting_attrs_keeps_its_rows_and_shortens_values():
    """No database: a search page over its budget while `select` names attrs
    keeps every row with its attribute strings shortened (D4); without attrs
    selected the page is still cut by rows."""
    import inspect
    import json

    from okg.substrate.mcp import response_bounds

    def operator(limit: int = 6, offset: int = 0, select=None) -> dict:
        return {"results": [{"node_id": f"t{i}", "attrs": {"text": "x" * 900}}
                            for i in range(offset, offset + limit)],
                "limit": limit, "offset": offset, "next_offset": offset + limit}

    def run(select):
        arguments = {"limit": 6, "offset": 0, "select": select}
        signature = inspect.signature(operator)
        return response_bounds._fit_to_budget(
            "search", operator, bound=signature.bind(**arguments), signature=signature,
            call_arguments=dict(arguments), arguments=dict(arguments), budget=2500,
            measure=lambda payload: len(json.dumps(payload)),
        )

    # read-passage-rows-3666 (D4): a search page whose `select` names attrs
    # is no longer cut to fewer rows: every requested row is served, its
    # attribute strings shortened to one length that fits, which the page
    # states. Before, this page held fewer than 6 rows.
    shortened = run(["node_id", "attrs"])
    assert [row["node_id"] for row in shortened["results"]] == [f"t{i}" for i in range(6)]
    kept = shortened["passages_shortened_to"]
    assert all(len(row["attrs"]["text"]) <= kept < 900 for row in shortened["results"])
    assert "truncation_remedy" not in shortened
    # Without attrs in select the attributes are not shortened: the page is
    # cut by rows as before, with no remedy.
    cut = run(["node_id"])
    assert cut["effective_limit"] < 6
    assert "truncation_remedy" not in cut
