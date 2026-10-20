"""A 1-hop `expand` row names its neighbour's type and title (#2675, #3072).

An expand row carried only the neighbour's id (`dst`, or `src` coming in), so
an agent saw a list of opaque ids and did not follow links: 0 expand calls in
946 in one benchmark run, where a `related_page` edge led straight to the gold
record. Each default row now carries `neighbor: {subtype, title}` (its id is the row's `dst` or `src`),
read like a passage's parent card: at the session's pin, through the Nomos
read filter, with effective attrs and the parent-card title rule. The text
card prints it on the row's line. A neighbour the reader may not read gets no
summary, exactly like one that does not exist. `neighbor` comes on every row,
whatever `select` names, like the edge's own identity fields; it is not a
`select` field, so the tool schema does not change.
"""
from __future__ import annotations

import json

import psycopg
import pytest

from okg.substrate.mcp import open_session
from okg.substrate.mcp.result_text import render
from okg.substrate.mcp.tools import expand_neighbors
from tests.substrate.mcp.test_search_records import (
    MAIN,
    OPEN,
    READER,
    _activate,
    _allow,
    _edge,
    _node,
    _nomos_facts,
    _publish,
)


FENCED = "⟦UNTRUSTED_DATA source=system:analyst_search_fixture⟧\n{}\n⟦/UNTRUSTED_DATA⟧"


def _facts() -> list:
    return [
        _node("ticket:weld", "ticket", "Weld review.", title="Weld failure review"),
        _node("comment:weld:0", "comment", "First look.", title="Inspection notes"),
        _node("comment:weld:1", "comment", "No title here."),
        _edge("ticket:weld", "comment:weld:0"),
        _edge("ticket:weld", "comment:weld:1"),
    ]


@pytest.fixture
def plain(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_facts())
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        yield conn, open_session(conn, deployment_name=deployment), deployment


def _by_far_end(page: dict) -> dict:
    return {
        (row["dst"] if row.get("direction") == "out" else row["src"]): row
        for row in page["neighbors"]
    }


@pytest.mark.postgres
def test_each_row_names_its_neighbour_and_the_text_line_shows_it(plain):
    conn, session, _ = plain
    page = expand_neighbors(conn, session, "ticket:weld")
    rows = _by_far_end(page)
    # The fixture's source is untrusted, so the title arrives fenced, exactly
    # as a parent card's title does.
    assert rows["comment:weld:0"]["neighbor"] == {
        "subtype": "comment", "title": FENCED.format("Inspection notes"),
    }
    # No title attribute: the block still names the type.
    assert rows["comment:weld:1"]["neighbor"] == {"subtype": "comment", "title": None}
    text = render(dict(page, operator_name="expand"), tool="expand")
    # The type goes on the row's line; a fenced (untrusted) title never
    # does: it is a block under the row, as a parent card's title is.
    (line,) = [ln for ln in text.splitlines() if "→ comment:weld:0" in ln]
    assert line.startswith("[1] contains → comment:weld:0 [comment] · edge ")
    assert "UNTRUSTED_DATA" not in line
    assert "--- neighbor title" in text and "Inspection notes" in text
    assert "contains → comment:weld:1 [comment] · edge " in text
    assert "neighbor:" not in text, "said once, not repeated as a field"

    # Coming in, the neighbour is the source.
    incoming = expand_neighbors(conn, session, "comment:weld:0", direction="in")
    (row,) = incoming["neighbors"]
    assert row["src"] == "ticket:weld" and row["neighbor"]["subtype"] == "ticket"
    assert row["neighbor"]["title"] == FENCED.format("Weld failure review")


@pytest.mark.postgres
def test_a_narrowed_select_still_names_the_neighbour(plain):
    """Like the edge's own identity fields, `neighbor` comes whatever select
    names: in lane Q's confirm run agents narrowed select on 25 of 44 one-hop
    calls, and those rows had carried no title."""
    conn, session, _ = plain
    page = expand_neighbors(conn, session, "ticket:weld", select=["edge_id", "dst"])
    rows = _by_far_end(page)
    assert set(rows["comment:weld:0"]) == {
        "edge_id", "src", "dst", "edge_type", "direction", "neighbor",
    }
    assert rows["comment:weld:0"]["neighbor"]["subtype"] == "comment"


@pytest.mark.postgres
def test_the_filtered_and_ordered_one_hop_path_names_neighbours_too(
    plain, clean_runtime,
):
    """`order_by` routes 1-hop expand through the effective-attribute read
    (`_expand_one_hop_effective`), served over MCP."""
    from tests.substrate.mcp.test_search_list import Served

    _, _, deployment = plain
    served = Served(clean_runtime, deployment)
    try:
        import anyio
        from mcp.shared.memory import create_connected_server_and_client_session

        out: list = []

        async def drive() -> None:
            async with create_connected_server_and_client_session(served.mcp) as client:
                # Issue #3666: only the link's own names ask for link rows.
                links = ["edge_id", "src", "dst", "edge_type", "direction"]
                for arguments in (
                    {"input": {"node_id": "ticket:weld"}, "order_by": "node_id",
                     "select": links},
                    {"input": {"node_id": "comment:weld:0"}, "direction": "in",
                     "order_by": "node_id", "select": links},
                ):
                    out.append(await client.call_tool(
                        "expand", arguments, meta={"okg/result_format": "json"},
                    ))

        anyio.run(drive)
    finally:
        served.close()
    page = json.loads(out[0].content[0].text)
    rows = _by_far_end(page)
    assert rows["comment:weld:0"]["neighbor"]["title"] == FENCED.format("Inspection notes")
    (incoming,) = json.loads(out[1].content[0].text)["neighbors"]
    assert incoming["src"] == "ticket:weld"
    assert incoming["neighbor"] == {
        "subtype": "ticket", "title": FENCED.format("Weld failure review"),
    }


@pytest.mark.postgres
def test_under_enforce_a_hidden_neighbour_gets_no_summary(
    clean_runtime, tmp_path, monkeypatch,
):
    """`ticket:open -[contains]-> comment:secret:1`: the edge is in a readable
    source, its far end in a source the reader may not read. The row is
    served for its edge, but nothing about the hidden node is: no summary,
    and none of its text."""
    groups, main = _nomos_facts()
    deployment = _publish(tmp_path, clean_runtime, groups=groups, main=main)
    _activate(clean_runtime, deployment, [_allow(OPEN), _allow(MAIN)])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment, nomos_subject=READER)
        page = expand_neighbors(conn, session, "ticket:open")
    rows = _by_far_end(page)
    assert rows["comment:open:0"]["neighbor"]["subtype"] == "comment"
    # Same keys on every row (#903); a hidden neighbour reads as missing.
    assert rows["comment:secret:1"]["neighbor"] is None
    served = json.dumps(page) + render(dict(page, operator_name="expand"), tool="expand")
    assert "secret finding" not in served


def test_a_trusted_title_goes_on_the_line_and_a_fenced_one_is_a_block() -> None:
    """The renderer alone: a plain title is quoted on the row's line; a
    fenced one is never on a line with the edge's facts."""
    fenced = FENCED.format("Inspection notes")
    page = {
        "operator_name": "expand", "node_id": "t:1", "direction": "out",
        "neighbors": [
            {"edge_id": 1, "src": "t:1", "dst": "c:1", "edge_type": "contains",
             "direction": "out", "neighbor": {"subtype": "comment", "title": "Plain (title)"}},
            {"edge_id": 2, "src": "t:1", "dst": "c:2", "edge_type": "contains",
             "direction": "out", "neighbor": {"subtype": "comment", "title": fenced}},
            {"edge_id": 3, "src": "t:1", "dst": "c:3", "edge_type": "contains",
             "direction": "out", "neighbor": None},
        ],
    }
    lines = render(page, tool="expand").splitlines()
    assert '[1] contains → c:1 [comment] "Plain (title)" · edge 1' in lines
    (second,) = [ln for ln in lines if "→ c:2" in ln]
    assert second == "[2] contains → c:2 [comment] · edge 2"
    assert any(ln.startswith("--- neighbor title") for ln in lines)
    assert all("UNTRUSTED_DATA" not in ln or not ln.startswith("[") for ln in lines)
    # An unreadable or missing neighbour: just the edge, no stray field.
    assert "[3] contains → c:3 · edge 3" in lines
    assert not [ln for ln in lines if ln.startswith("neighbor")]
