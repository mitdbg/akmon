"""`expand` returns the linked records (issue #3666), through the served tool.

One published graph with two starting records. `doc:with` links to four
passages; `doc:without` links to the same passages but one, `doc_chunk:p2`,
which comes from a source one reader is denied. Passage `p3` is reached by
two links from each. So the two starting records differ only in a far
record that reader may not see, and their replies to that reader must be
the same reply.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import psycopg
import pytest

from okg.substrate.graph_store_internal.common import edge_id
from okg.substrate.library.sources.base import EdgeFact
from okg.substrate.mcp import open_session
from okg.substrate.mcp.provenance import is_fenced
from okg.substrate.mcp.response_shape import compact_for_wire
from okg.substrate.mcp.result_text import compact_json, render
from okg.substrate.mcp.tools import expand_neighbors
from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes
from tests.substrate.mcp.test_read_grammar_db import Tools, _same, _served, _wire_bytes
from tests.substrate.mcp.test_search_list import (
    DOC,
    DOC_CHUNK,
    EXTERNAL,
    MAIN,
    READER,
    _activate,
    _edge,
    _mask_doc_title_and_path,
    _node,
    _trust,
)

pytestmark = pytest.mark.postgres

WITH, WITHOUT = "doc:with", "doc:without"
P1, P2, P3, P4 = (f"doc_chunk:p{n}" for n in (1, 2, 3, 4))
WIDE = "doc:wide"
WIDE_PASSAGES = [f"doc_chunk:w{n:02d}" for n in range(60)]
RECORD_KEYS = {"node_id", "subtype", "title", "link", "link_count"}
LINK_NAMES = ["edge_id", "src", "dst", "edge_type", "direction"]

_DENY_EXTERNAL = [
    {"rule_id": "deny_external", "effect": "deny", "actions": ["graph.read.mcp"],
     "subjects": [READER], "match": {"source_id": EXTERNAL}, "priority": 200},
    {"rule_id": "allow_read", "effect": "allow",
     "actions": ["graph.read.mcp", "mcp.tool.call"], "subjects": [READER], "priority": 100},
]


def _mention(src: str, dst: str) -> EdgeFact:
    return EdgeFact(
        src=src, dst=dst, edge_type="mentions", attrs={"role": "cites"},
        source_record_id={"id": f"{src}|mentions|{dst}"}, source_revision={"v": 1},
    )


def _fan(tmp_path, dsn: str) -> str:
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    publish_nodes(
        dsn, deployment, subtypes=[DOC, DOC_CHUNK],
        narrowings=[("doc", "contains", "doc_chunk"), ("doc", "mentions", "doc_chunk")],
        nodes=[],
        node_groups=[(MAIN, [
            _node(WITH, "doc", "the record with the extra passage"),
            _node(WITHOUT, "doc", "the record without it"),
            _node(P1, "doc_chunk", "passage one", kind="odd"),
            _node(P3, "doc_chunk", "passage three", kind="odd"),
            _node(P4, "doc_chunk", "passage four", kind="even"),
            _node(WIDE, "doc", "a record with more passages than one small page holds"),
            *(_node(far, "doc_chunk", f"wide passage {far} " + "filler " * 40)
              for far in WIDE_PASSAGES),
        ]), (EXTERNAL, [
            _node(P2, "doc_chunk", "the passage one reader may not see", kind="even"),
            # A second link to a passage that reader MAY see, itself from the
            # denied source: a hidden link to a visible record.
            _mention(WITH, P4),
        ])],
        edges=[
            *(_edge(hub, far) for hub in (WITH, WITHOUT) for far in (P1, P3, P4)),
            *(_mention(hub, P3) for hub in (WITH, WITHOUT)),
            *(_edge(WIDE, far) for far in WIDE_PASSAGES),
            # Last, so no other link's id depends on it.
            _edge(WITH, P2),
        ],
    )
    _trust(dsn, deployment, MAIN)
    return deployment


@pytest.fixture
def fan(clean_runtime, okg_mcp_can_log_in, tmp_path):
    server = Tools(clean_runtime, _fan(tmp_path, clean_runtime))
    try:
        yield server
    finally:
        server.close()


def _reply(server: Tools, arguments: dict) -> dict:
    (result,) = server.run(("expand", arguments, "json"))
    return json.loads(result.content[0].text)


def _expand(server: Tools, node_id: str = WITH, **arguments) -> dict:
    return _served(_reply(server, {"input": {"node_id": node_id}, **arguments}))


def _ids(reply: dict) -> list[str]:
    return [row["node_id"] for row in reply["neighbors"]]


_ROW_LINE = re.compile(r"\[\d+\] |  ")


def _row_lines(text: str) -> list[str]:
    """The lines of a text reply that print its rows: each row's head line
    and the indented lines under it."""
    return [line for line in text.splitlines() if _ROW_LINE.match(line)]


def test_default_rows_are_the_linked_records(fan):
    reply = _expand(fan)
    assert _ids(reply) == [P1, P2, P3, P4]
    for row in reply["neighbors"]:
        assert set(row) == RECORD_KEYS and row["subtype"] == "doc_chunk"
        assert set(row["link"]) == {"edge_type", "direction", "edge_id"}
        assert row["link"]["direction"] == "out" and isinstance(row["link"]["edge_id"], int)
    # No attributes unless selected: the far record's or the link's.
    assert "attrs" not in json.dumps(reply["neighbors"])
    text = fan.text("expand", {"input": {"node_id": WITH}})
    rows = [line for line in text.splitlines() if line.startswith("[")]
    assert len(rows) == 4 and rows[0].startswith(f"[1] {P1} (doc_chunk)")
    assert "via contains →" in rows[0] and "→ null" not in text


def test_a_record_reached_by_several_links_is_one_row_that_says_how_many(fan):
    rows = {row["node_id"]: row for row in _expand(fan)["neighbors"]}
    assert rows[P3]["link_count"] == 2
    # (With access rules off, `doc:with` also reaches p4 twice.)
    assert [rows[far]["link_count"] for far in (P1, P2, P4)] == [1, 1, 2]
    # The link shown is chosen by link type, then id: here the `contains`.
    assert rows[P3]["link"]["edge_type"] == rows[P4]["link"]["edge_type"] == "contains"
    # Narrowed to one link type there is one link, and it is that one.
    (only, other) = _expand(fan, edge_type="mentions")["neighbors"]
    assert only["node_id"] == P3 and only["link_count"] == 1 and other["node_id"] == P4
    assert only["link"]["edge_type"] == "mentions"
    # Coming in, the far records are the two starting records.
    back = _expand(fan, P3, direction="in")
    assert _ids(back) == [WITH, WITHOUT]
    assert [row["link_count"] for row in back["neighbors"]] == [2, 2]
    assert {row["link"]["direction"] for row in back["neighbors"]} == {"in"}
    # Both directions: the same four records, each once.
    assert _ids(_expand(fan, direction="both")) == [P1, P2, P3, P4]


def test_select_chooses_the_far_records_fields(fan):
    reply = _expand(fan, select=["node_id", "subtype", "attrs", "provenance"])
    by_id = {row["node_id"]: row for row in reply["neighbors"]}
    assert set(by_id[P1]) == RECORD_KEYS | {"attrs", "provenance"}
    # The far record's own attributes, not the link's.
    assert by_id[P1]["attrs"]["kind"] == "odd" and "passage one" in json.dumps(by_id[P1]["attrs"])
    assert "role" not in by_id[P3]["attrs"]
    # The link's attributes and ends are asked for by name, and sit in `link`.
    (mention, _other) = _expand(
        fan, edge_type="mentions", select=["node_id", "link.attrs", "src", "dst"],
    )["neighbors"]
    assert mention["link"]["src"] == WITH and mention["link"]["dst"] == P3
    assert mention["link"]["attrs"].get("role") == "cites"
    assert "attrs" not in mention
    lineage = _expand(fan, select=["node_id", "source_lineage"])
    assert all("source_lineage" in row and "attrs" not in row for row in lineage["neighbors"])
    # Failure case: a name that is no field of a record row is refused.
    (refused,) = fan.run(("expand", {"input": {"node_id": WITH}, "select": ["node_id", "score"]},
                          "json"))
    assert refused.isError
    assert json.loads(refused.content[0].text)["error"]["code"] == "select_unknown_field"


def test_a_select_of_only_link_fields_returns_the_link_rows_as_before(fan, clean_runtime):
    """One row per link, not per record. The reference is the link read this
    step did not change (`expand_neighbors`), put on the wire by the
    server's own functions: the rows of the JSON reply are its rows exactly,
    and the rows of the text reply print as its rows print."""
    for arguments in (
        {"select": LINK_NAMES},
        {"select": ["dst"], "edge_type": "contains"},
        # The old spelling: beside a link field, `attrs` is the link's own.
        {"select": [*LINK_NAMES, "attrs", "read_provenance"]},
        {"select": ["dst", "attrs"], "edge_type": "mentions"},
    ):
        select = arguments["select"]
        call = {"input": {"node_id": WITH}, **arguments}
        reply = _served(_reply(fan, call))
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = open_session(conn, deployment_name=fan.deployment)
            # The tool's own default page size, which this step left at 50.
            assert reply["limit"] == 50, arguments
            before = expand_neighbors(
                conn, session, WITH, edge_type=arguments.get("edge_type"), select=select,
                limit=reply["limit"],
            )
        reference = json.loads(compact_json(compact_for_wire(
            {"operator_name": "expand", **before}, {"select": select},
        )))
        assert reply["neighbors"], arguments
        assert reply["neighbors"] == reference["neighbors"], arguments
        for key in ("total_count", "total_count_basis", "truncated", "next_offset",
                    "returned_count", "offset", "limit", "direction", "node_id"):
            assert reply.get(key) == reference.get(key), (key, arguments)
        assert all("node_id" not in row and "link" not in row for row in reply["neighbors"])
        # The text reply prints those rows as the reference rows print.
        printed = render({**reply, "neighbors": reference["neighbors"]},
                         tool="expand", select=select)
        assert _row_lines(fan.text("expand", call)) == _row_lines(printed), arguments
        assert _row_lines(printed), arguments
        # Only a call that names `attrs` is told whose they are.
        noted = [record["rule"] for record in reply.get("adjusted", [])]
        assert noted == (["link_rows_attrs"] if "attrs" in select else []), arguments
    # The link's attributes by their new name: the same rows, and no note.
    old = {"input": {"node_id": WITH}, "select": [*LINK_NAMES, "attrs", "read_provenance"]}
    new = {"input": {"node_id": WITH}, "select": [*LINK_NAMES, "link.attrs", "read_provenance"]}
    old_reply, new_reply = _served(_reply(fan, old)), _served(_reply(fan, new))
    assert "adjusted" not in new_reply
    assert _same(new_reply) == _same(old_reply, without=("adjusted",))
    assert _row_lines(fan.text("expand", new)) == _row_lines(fan.text("expand", old))
    assert "! adjusted: select `attrs` read as each link's own attrs" in fan.text("expand", old)
    assert "! adjusted" not in fan.text("expand", new)
    # Six links reach four records: the link view does not fold them.
    assert len(_expand(fan, select=LINK_NAMES)["neighbors"]) == 6
    # Failure case: one record field beside them and the rows are records.
    mixed = _expand(fan, select=[*LINK_NAMES, "node_id"])
    assert len(mixed["neighbors"]) == 4 and "adjusted" not in mixed


def test_where_is_a_condition_on_the_far_record(fan):
    odd = _expand(fan, where={"type": "doc_chunk", "attrs.kind": "odd"})
    assert _ids(odd) == [P1, P3] and odd["total_count"] == 2
    assert all(set(row) == RECORD_KEYS for row in odd["neighbors"])
    # The long form, the older argument and `target_subtype` say the same.
    long_form = _expand(fan, where={"op": "eq", "field": "attrs.kind", "value": "even"})
    assert _ids(long_form) == [P2, P4]
    assert _ids(_expand(fan, filter={"op": "eq", "field": "attrs.kind", "value": "even"})) == [P2, P4]
    assert _ids(_expand(fan, target_subtype="doc_chunk")) == [P1, P2, P3, P4]
    # With a condition a record reached twice is still one row.
    (third,) = [row for row in odd["neighbors"] if row["node_id"] == P3]
    assert third["link_count"] == 2
    # Failure cases: a type nothing has, and a far-record field that is none.
    none = _expand(fan, where={"type": "ticket"})
    assert none["neighbors"] == [] and none["total_count"] == 0
    (refused,) = fan.run(("expand", {"input": {"node_id": WITH}, "where": {"colour": "red"}},
                          "json"))
    assert refused.isError


def test_the_reply_counts_the_far_records(fan):
    whole = _expand(fan)
    assert whole["total_count"] == 4 and whole["total_count_basis"] == "exact"
    assert whole["truncated"] is False and whole["next_offset"] is None
    # Not the number of links (five), and not the page size.
    cut = _expand(fan, limit=1)
    assert cut["total_count"] == 4 and len(cut["neighbors"]) == 1
    assert _expand(fan, WIDE)["total_count"] == 60
    assert _expand(fan, WITHOUT)["total_count"] == 3
    assert _expand(fan, edge_type="mentions")["total_count"] == 2
    # Failure case: a record with no link of that type counts none.
    empty = _expand(fan, P1, edge_type="contains")
    assert empty["neighbors"] == [] and empty["total_count"] == 0


@pytest.mark.parametrize("extra", [{}, {"order_by": "node_id"}, {"where": {"type": "doc_chunk"}}])
def test_pages_continue_the_record_rows(fan, extra):
    seen: list[str] = []
    offset, pages = 0, 0
    while offset is not None:
        page = _expand(fan, limit=3, offset=offset, **extra)
        assert all(set(row) == RECORD_KEYS for row in page["neighbors"])
        seen += _ids(page)
        pages += 1
        assert pages < 5, "the cursor did not end"
        assert (page["next_offset"] is None) == (not page["truncated"])
        offset = page["next_offset"]
    # Every record once, in record order, over two pages.
    assert seen == [P1, P2, P3, P4] and pages == 2


def test_a_small_budget_serves_fewer_records_and_says_how_to_continue(fan):
    call = {"input": {"node_id": WIDE}, "limit": 100,
            "select": ["node_id", "subtype", "attrs", "provenance"]}
    for result_format in ("json", "text"):
        (result,) = fan.run(("expand", {**call, "max_bytes": 4096}, result_format))
        assert not result.isError and _wire_bytes(result) <= 4096
    (result,) = fan.run(("expand", {**call, "max_bytes": 4096}, "json"))
    small = _served(json.loads(result.content[0].text))
    shown = len(small["neighbors"])
    # Precondition: sixty records with their text are more than one page.
    assert 0 < shown < 60
    assert _ids(small) == WIDE_PASSAGES[:shown]
    # Offsets count records, so the cursor is the number of rows served.
    assert small["truncated"] is True and small["next_offset"] == shown
    following = _expand(fan, WIDE, limit=1, offset=small["next_offset"])
    assert _ids(following) == [WIDE_PASSAGES[shown]]


_PAIR_CALLS: list[dict[str, Any]] = [
    {},
    {"select": ["node_id", "subtype", "attrs", "provenance"]},
    {"select": ["node_id", "link.attrs", "src", "dst"]},
    {"direction": "both"},
    {"edge_type": "mentions"},
    {"edge_type": "mentions", "select": ["node_id", "link.attrs"]},
    {"edge_type": "contains", "limit": 1},
    {"edge_type": "contains", "limit": 1, "offset": 1},
    {"edge_type": "contains", "limit": 1, "offset": 2},
    {"where": {"type": "doc_chunk"}},
    {"where": {"attrs.kind": "even"}},
    {"where": {"attrs.kind": "even"}, "limit": 1},
    {"order_by": "node_id", "limit": 2},
    {"order_by": "node_id", "limit": 2, "offset": 2},
    {"target_subtype": "doc_chunk"},
]


def _neutral(body: str, starts: tuple[str, ...] = (WITHOUT, WITH)) -> str:
    """A reply with the starting record's id and the link ids taken out:
    the only things the two starting records' replies may differ in."""
    for start in starts:
        body = body.replace(start, "<start>")
    body = re.sub(r'"edge_id": \d+', '"edge_id": 0', body)
    return re.sub(r"\bedge \d+", "edge 0", body)


def test_a_far_record_the_reader_may_not_see_changes_nothing_in_the_reply(
        clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    """Under enforcement, the reply about the record that links to the
    hidden passage is the reply about the record that does not: the same
    rows in the same order, the same `link_count`, the same total, basis,
    paging and notes, in JSON and in text."""
    deployment = _fan(tmp_path, clean_runtime)
    _activate(clean_runtime, deployment, _DENY_EXTERNAL)

    def replies(server: Tools, calls=_PAIR_CALLS) -> list[tuple[str, str]]:
        out = []
        for call in calls:
            pair = []
            for start in (WITH, WITHOUT):
                arguments = {"input": {"node_id": start}, **call}
                reply = _served(_reply(server, arguments))
                pair.append(_neutral(json.dumps(_same(reply), sort_keys=True))
                            + "\n" + _neutral(server.text("expand", arguments)))
            out.append((pair[0], pair[1]))
        return out

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    server = Tools(clean_runtime, deployment)
    try:
        enforced = replies(server)
        counted = _served(_reply(server, {"input": {"node_id": WITH}}))
        typed = {"input": {"node_id": WITH}, "where": {"type": "doc_chunk"}, "limit": 1}
        first_page = _served(_reply(server, typed))
        later_page = _served(_reply(server, {**typed, "offset": 1}))
    finally:
        server.close()
    for call, (with_hidden, without) in zip(_PAIR_CALLS, enforced):
        assert with_hidden == without, call
        assert P2 not in with_hidden and "may not see" not in with_hidden, call
    assert _ids(counted) == [P1, P3, P4] and counted["total_count"] == 3
    assert counted["total_count_basis"] == "exact_visible"
    # The hidden second link to p4 is not counted.
    assert [row["link_count"] for row in counted["neighbors"]] == [1, 2, 1]
    # With a condition, the count under enforcement is page-only, and the
    # reply says so in the words `filter` uses: at least this many on a first
    # page that was cut, unknown on a later one. Never a guessed total.
    assert (first_page["total_count"], first_page["total_count_basis"]) == (1, "lower_bound")
    assert first_page["truncated"] is True
    assert (later_page["total_count"], later_page["total_count_basis"]) == (None, "unknown")
    # Failure case: with enforcement off the same calls differ, so the
    # equality above comes from the policy and not from the comparison.
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    monkeypatch.delenv("OKG_NOMOS_SUBJECT", raising=False)
    server = Tools(clean_runtime, deployment)
    try:
        # The control asserts on the first call only, so serve that one.
        opened = replies(server, _PAIR_CALLS[:1])
    finally:
        server.close()
    assert opened[0][0] != opened[0][1] and P2 in opened[0][0]


@pytest.mark.parametrize("mode", ["audit", "enforce"])
def test_a_denied_record_is_hidden_under_enforce_and_served_as_filter_serves_it_under_audit(
        clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch, mode):
    """A bundle denies the reader one source. Under enforce, the passage
    from that source is never a linked record, is in no `link_count` and
    no total, and the reply about the record that links to it is byte for
    byte the reply about its twin, whose part of the graph is the same
    without that passage and its links (JSON and text, once the starting
    record's id and link ids are taken out).

    Under audit the per-row check records the denial and serves the row,
    for every read tool: expand serves exactly what `filter` serves under
    the same bundle, the passage included, and no more."""
    deployment = _fan(tmp_path, clean_runtime)
    _activate(clean_runtime, deployment, _DENY_EXTERNAL)
    calls: list[dict[str, Any]] = [
        *_PAIR_CALLS, {"where": {"attrs.kind": "even"}}, {"order_by": "node_id"},
        {"select": ["node_id", "attrs", "link.attrs"]},
    ]
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    server = Tools(clean_runtime, deployment)
    if mode == "audit":
        try:
            linked = _served(_reply(server, {"input": {"node_id": WITH}}))
            # Read the decisions before `filter` runs: `filter` records its
            # own deny for P2 under audit, so only expand's row counts here.
            with psycopg.connect(clean_runtime) as conn:
                effects = {str(effect) for (effect,) in conn.execute(
                    "SELECT effect FROM okg.nomos_decisions "
                    "WHERE target->>'node_id' = %s", (P2,),
                ).fetchall()}
            (result,) = server.run(("filter", {
                "input": {"kind": "nodes", "subtype": "doc_chunk"}, "predicate": {},
                "limit": 100,
            }, "json"))
            chunks = _served(json.loads(result.content[0].text))
        finally:
            server.close()
        filtered = {row["node_id"] for row in chunks["items"]}
        assert P2 in filtered, "audit serves the denied passage through filter too"
        assert _ids(linked) == sorted({P1, P2, P3, P4} & filtered)
        assert linked["total_count"] == 4
        # The denied link to p4 is served too, so p4 is reached twice.
        by_id = {row["node_id"]: row for row in linked["neighbors"]}
        assert by_id[P4]["link_count"] == 2
        # Expand's check recorded the denial it did not apply.
        assert any("deny" in effect for effect in effects), effects
        return
    try:
        for call in calls:
            pair, served = [], []
            for start in (WITH, WITHOUT):
                arguments = {"input": {"node_id": start}, **call}
                reply = _served(_reply(server, arguments))
                served.append(reply)
                pair.append(_neutral(json.dumps(_same(reply), sort_keys=True))
                            + "\n" + _neutral(server.text("expand", arguments)))
            assert pair[0] == pair[1], (mode, call)
            assert P2 not in pair[0] and "may not see" not in pair[0], (mode, call)
            assert served[0].get("total_count") == served[1].get("total_count"), call
        whole = _served(_reply(server, {"input": {"node_id": WITH}}))
    finally:
        server.close()
    assert _ids(whole) == [P1, P3, P4] and whole["total_count"] == 3, mode
    # The hidden second link to p4 is in no link count.
    assert [row["link_count"] for row in whole["neighbors"]] == [1, 2, 1], mode
    # Failure case: with access rules off the denied passage is a linked
    # record of `doc:with`, so the twin replies differ.
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    monkeypatch.delenv("OKG_NOMOS_SUBJECT", raising=False)
    server = Tools(clean_runtime, deployment)
    try:
        opened = _served(_reply(server, {"input": {"node_id": WITH}}))
    finally:
        server.close()
    assert P2 in _ids(opened) and opened["total_count"] == 4


def _replayed() -> list[dict]:
    data = json.loads(
        (Path(__file__).parent / "data" / "select_refusals_3666.json").read_text()
    )
    return [row["arguments"] for row in data["rows"] if row["tool"] == "expand"]


REPLAY_START = "doc:000"
REPLAY_PASSAGES = [f"doc_chunk:000-{n}" for n in range(3)]
REPLAY_HOLDER = "doc_chunk:holder"


def _replay_graph(tmp_path, dsn: str) -> str:
    """The record the replayed calls start from, with three passages going
    out and one `contains` link coming in from a passage, so the calls with
    direction "in" (one with a `doc_chunk` condition) have a row to check."""
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    publish_nodes(
        dsn, deployment, subtypes=[DOC, DOC_CHUNK],
        narrowings=[("doc", "contains", "doc_chunk"), ("doc_chunk", "contains", "doc")],
        nodes=[],
        node_groups=[(MAIN, [
            _node(REPLAY_START, "doc", "the starting record", summary="Document 000"),
            *(_node(far, "doc_chunk", f"passage {far}") for far in REPLAY_PASSAGES),
            _node(REPLAY_HOLDER, "doc_chunk", "a passage that holds the record"),
        ])],
        edges=[
            *(_edge(REPLAY_START, far) for far in REPLAY_PASSAGES),
            _edge(REPLAY_HOLDER, REPLAY_START),
        ],
    )
    _trust(dsn, deployment, MAIN)
    return deployment


@pytest.fixture
def replay(clean_runtime, okg_mcp_can_log_in, tmp_path):
    server = Tools(clean_runtime, _replay_graph(tmp_path, clean_runtime))
    try:
        yield server
    finally:
        server.close()


def test_every_replayed_refused_call_is_now_served_with_the_fields_asked(replay):
    """The 21 `expand` calls agents made and the server refused, as 13
    shapes with neutral values. Each is dispatched as it was sent, each has
    rows, and every row has every field the call selected."""
    shapes = _replayed()
    assert len(shapes) == 13
    directions = {arguments["direction"] for arguments in shapes}
    assert directions == {"in", "out"}
    for arguments in shapes:
        (result,) = replay.run(("expand", arguments, "json"))
        assert not result.isError, (arguments, result.content[0].text[:400])
        reply = _served(json.loads(result.content[0].text))
        assert "adjusted" not in reply, arguments
        if "max_bytes" in arguments:
            assert len(result.content[0].text.encode("utf-8")) <= arguments["max_bytes"]
        # Rows first: a field assertion over no rows asserts nothing.
        assert reply["neighbors"], arguments
        outgoing = arguments["direction"] == "out"
        assert _ids(reply) == (REPLAY_PASSAGES if outgoing else [REPLAY_HOLDER]), arguments
        assert reply["total_count"] == (3 if outgoing else 1), arguments
        for row in reply["neighbors"]:
            assert set(row) >= RECORD_KEYS, arguments
            assert row["link"]["direction"] == arguments["direction"], arguments
            for name in arguments["select"]:
                holder = row["link"] if name in ("edge_type", "src", "dst") else row
                assert name in holder, (name, arguments)
            if "attrs" in arguments["select"]:
                # The record's own attributes, on every shape (the mixed
                # select with link fields beside `node_id` among them).
                assert "passage" in row["attrs"]["text"], arguments
            if "src" in arguments["select"]:
                assert (row["link"]["src"], row["link"]["dst"]) == (
                    (REPLAY_START, row["node_id"]) if outgoing
                    else (row["node_id"], REPLAY_START)
                ), arguments
    # The one mixed select is among the shapes, and comes in.
    assert any(
        arguments["direction"] == "in" and "src" in arguments["select"]
        and "attrs" in arguments["select"] for arguments in shapes
    )


def test_the_link_budget_counts_only_links_the_reader_may_see(
        clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    """Both starting records have four links the reader may see that reach
    a record it may see. `doc:with` has two more: a hidden link, and a link
    it may see to a record it may not. With the budget at four, neither is
    over it, and the two replies are the same: neither a hidden link nor a
    link to a hidden record takes a budget slot."""
    from okg.substrate.mcp import tools as tools_module

    deployment = _fan(tmp_path, clean_runtime)
    _activate(clean_runtime, deployment, _DENY_EXTERNAL)
    monkeypatch.setattr(tools_module, "EXPAND_LINK_BUDGET", 4)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    server = Tools(clean_runtime, deployment)
    try:
        for call in ({}, {"where": {"type": "doc_chunk"}}):
            pair = [
                _served(_reply(server, {"input": {"node_id": start}, **call}))
                for start in (WITH, WITHOUT)
            ]
            for reply in pair:
                assert reply["total_count_basis"] != "omitted_over_link_budget", call
                assert "pagination_capped" not in reply and _ids(reply) == [P1, P3, P4]
            assert (_neutral(json.dumps(_same(pair[0]), sort_keys=True))
                    == _neutral(json.dumps(_same(pair[1]), sort_keys=True))), call
        # Failure case: one fewer and both are over it, and both say so.
        monkeypatch.setattr(tools_module, "EXPAND_LINK_BUDGET", 3)
        for start in (WITH, WITHOUT):
            over = _served(_reply(server, {"input": {"node_id": start}}))
            assert over["total_count_basis"] == "omitted_over_link_budget", start
            assert over["total_count"] is None and over["truncated"] is True
    finally:
        server.close()


START, TWIN = "doc:start", "doc:twin"
PLAIN, OUTSIDE = "doc:plain", "doc:outside"
NEAR = "doc_chunk:near"
OUTSIDE_TITLE = "Quarterly figures, outside copy"


def _typed(src: str, dst: str, edge_type: str) -> EdgeFact:
    return EdgeFact(
        src=src, dst=dst, edge_type=edge_type,
        source_record_id={"id": f"{src}|{edge_type}|{dst}"}, source_revision={"v": 1},
    )


def _mixed(tmp_path, dsn: str) -> str:
    """Two starting records that mention the same two documents, one from
    the trusted source and one from the external source, and contain the
    same passage. `doc:start` has one more link to that passage, from the
    external source, whose type (`about`) sorts before `contains`."""
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    publish_nodes(
        dsn, deployment, subtypes=[DOC, DOC_CHUNK],
        narrowings=[("doc", "contains", "doc_chunk"), ("doc", "about", "doc_chunk"),
                    ("doc", "mentions", "doc")],
        nodes=[],
        node_groups=[(MAIN, [
            _node(START, "doc", "the starting record", summary="Start"),
            _node(TWIN, "doc", "its twin", summary="Twin"),
            _node(PLAIN, "doc", "a plain body", summary="Plain title"),
            _node(NEAR, "doc_chunk", "a passage both contain"),
        ]), (EXTERNAL, [
            _node(OUTSIDE, "doc", "an outside body", summary=OUTSIDE_TITLE),
            _typed(START, NEAR, "about"),
        ])],
        edges=[
            *(_edge(hub, NEAR) for hub in (START, TWIN)),
            *(_typed(hub, far, "mentions") for hub in (START, TWIN) for far in (PLAIN, OUTSIDE)),
        ],
    )
    _trust(dsn, deployment, MAIN)
    return deployment


def test_an_untrusted_far_records_title_and_attrs_are_fenced(
        clean_runtime, okg_mcp_can_log_in, tmp_path):
    server = Tools(clean_runtime, _mixed(tmp_path, clean_runtime))
    try:
        call = {"input": {"node_id": START}, "edge_type": "mentions",
                "select": ["node_id", "attrs", "provenance"]}
        rows = {row["node_id"]: row for row in _served(_reply(server, call))["neighbors"]}
        text = server.text("expand", call)
    finally:
        server.close()
    outside, plain = rows[OUTSIDE], rows[PLAIN]
    assert outside["provenance"]["trust"] == "untrusted"
    assert is_fenced(outside["title"]) and OUTSIDE_TITLE in outside["title"]
    assert is_fenced(outside["attrs"]["text"]) and is_fenced(outside["attrs"]["summary"])
    # In text a fenced title is never printed as the row's quoted title.
    assert f'(doc) "{OUTSIDE_TITLE}"' not in text and '(doc) "Plain title"' in text
    # Failure case: the trusted record beside it is served bare.
    assert plain["provenance"]["trust"] == "trusted" and plain["title"] == "Plain title"
    assert not is_fenced(plain["attrs"]["text"])


def test_a_masked_attribute_selected_through_attrs_is_masked_as_inspect_masks_it(
        clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    deployment = _mixed(tmp_path, clean_runtime)
    _mask_doc_title_and_path(clean_runtime)
    _activate(clean_runtime, deployment, [
        {"rule_id": "redact_external", "effect": "redact", "actions": ["graph.read.mcp"],
         "subjects": [READER], "match": {"source_id": EXTERNAL}, "priority": 200,
         "obligations": [{"type": "redaction"}, {"type": "audit"}]},
        {"rule_id": "allow_read", "effect": "allow",
         "actions": ["graph.read.mcp", "mcp.tool.call"], "subjects": [READER], "priority": 100},
    ])
    call = {"input": {"node_id": START}, "edge_type": "mentions",
            "select": ["node_id", "attrs", "provenance"]}
    ordered = {**call, "order_by": "node_id"}

    def read(server: Tools) -> tuple[list[dict], dict, dict, str]:
        expanded = [_served(_reply(server, arguments)) for arguments in (call, ordered)]
        inspected, filtered = server.run(
            ("inspect", {"target": {"kind": "node", "node_id": OUTSIDE}}, "json"),
            ("filter", {"input": {"kind": "nodes"}, "limit": 50,
                        "select": ["node_id", "attrs"],
                        "predicate": {"op": "eq", "field": "subtype", "value": "doc"}}, "json"),
        )
        (item,) = [row for row in _served(json.loads(filtered.content[0].text))["items"]
                   if row["node_id"] == OUTSIDE]
        return (expanded, _served(json.loads(inspected.content[0].text)), item,
                server.text("expand", call))

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    server = Tools(clean_runtime, deployment)
    try:
        expanded, inspected, item, text = read(server)
    finally:
        server.close()
    for reply in expanded:
        (row,) = [row for row in reply["neighbors"] if row["node_id"] == OUTSIDE]
        # The stored value is nowhere in the reply: not in `attrs`, not in
        # the title that is read from it.
        assert OUTSIDE_TITLE not in json.dumps(reply)
        # And the masked attribute is what `inspect` and `filter` serve.
        assert row["attrs"].get("summary") == inspected["attrs"].get("summary")
        assert row["attrs"].get("summary") == item["attrs"].get("summary")
        # An attribute that is not masked is still served.
        assert "outside body" in row["attrs"]["text"]
    assert OUTSIDE_TITLE not in text
    # Failure case: with enforcement off the same call serves the value, so
    # the mask above comes from the policy.
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    monkeypatch.delenv("OKG_NOMOS_SUBJECT", raising=False)
    server = Tools(clean_runtime, deployment)
    try:
        opened = _served(_reply(server, call))
    finally:
        server.close()
    assert OUTSIDE_TITLE in json.dumps(opened)


def test_a_hidden_link_of_an_earlier_type_does_not_choose_the_shown_link(
        clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    """`doc:start` reaches the passage by `contains` and by a hidden `about`
    link, which sorts first. Under enforcement the row shows `contains` and
    counts one link, and every reply is the twin's reply."""
    deployment = _mixed(tmp_path, clean_runtime)
    _activate(clean_runtime, deployment, _DENY_EXTERNAL)
    calls: list[dict[str, Any]] = [
        {}, {"direction": "both"}, {"select": ["node_id", "link.attrs", "src", "dst"]},
        {"where": {"type": "doc_chunk"}}, {"order_by": "node_id", "limit": 1},
        {"edge_type": "about"},
    ]

    def near_row(server: Tools) -> dict:
        (row,) = [row for row in _expand(server, START)["neighbors"] if row["node_id"] == NEAR]
        return row

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    server = Tools(clean_runtime, deployment)
    try:
        for call in calls:
            pair = []
            for start in (START, TWIN):
                arguments = {"input": {"node_id": start}, **call}
                reply = _served(_reply(server, arguments))
                pair.append(
                    _neutral(json.dumps(_same(reply), sort_keys=True), (START, TWIN))
                    + "\n" + _neutral(server.text("expand", arguments), (START, TWIN))
                )
            assert pair[0] == pair[1], call
            assert OUTSIDE not in pair[0], call
        shown = near_row(server)
    finally:
        server.close()
    assert shown["link"]["edge_type"] == "contains" and shown["link_count"] == 1
    # Failure case: with access rules off the `about` link is seen, it is
    # the one shown (link type decides, in SQL as in Python), and it counts.
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    monkeypatch.delenv("OKG_NOMOS_SUBJECT", raising=False)
    server = Tools(clean_runtime, deployment)
    try:
        opened = near_row(server)
        typed = _expand(server, START, where={"type": "doc_chunk"})["neighbors"]
    finally:
        server.close()
    assert opened["link"]["edge_type"] == "about" and opened["link_count"] == 2
    assert [(row["link"]["edge_type"], row["link_count"]) for row in typed] == [("about", 2)]


HUB_A, HUB_B = "doc:hub-a", "doc:hub-b"


def _same_order(count: int) -> list[str]:
    """`count` passage ids whose `contains` links from the two hubs sort the
    same way by link id. Link ids are hashes of the link, so above the
    budget two hubs serve the same records only when their links agree in
    id order; this picks passages for which they do."""
    candidates = [f"doc_chunk:h{n:03d}" for n in range(120)]
    by_a = sorted(candidates, key=lambda far: edge_id(HUB_A, "contains", far))
    in_b = [edge_id(HUB_B, "contains", far) for far in by_a]
    # Longest run of passages rising in both orders.
    best = [[index] for index in range(len(by_a))]
    for index in range(len(by_a)):
        for earlier in range(index):
            if in_b[earlier] < in_b[index] and len(best[earlier]) + 1 > len(best[index]):
                best[index] = [*best[earlier], index]
    longest = max(best, key=len)
    assert len(longest) >= count, "not enough passages in one order for both hubs"
    return [by_a[index] for index in longest[:count]]


def test_above_the_budget_hidden_links_and_records_change_nothing(
        clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    """Two hubs with the same six links to the same six passages a reader
    may see, in the same link-id order. Hub A also has links to four
    passages the reader may not see, and a hidden second link to one it
    may. With the budget at three both are over it, and every reply about
    hub A is the reply about hub B, in JSON and in text."""
    from okg.substrate.mcp import tools as tools_module

    seen = _same_order(6)
    hidden = [f"doc_chunk:x{n}" for n in range(4)]
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment, subtypes=[DOC, DOC_CHUNK],
        narrowings=[("doc", "contains", "doc_chunk"), ("doc", "mentions", "doc_chunk")],
        nodes=[],
        node_groups=[(MAIN, [
            _node(HUB_A, "doc", "hub a"), _node(HUB_B, "doc", "hub b"),
            *(_node(far, "doc_chunk", f"passage {far}", kind="odd") for far in seen),
        ]), (EXTERNAL, [
            *(_node(far, "doc_chunk", f"hidden passage {far}", kind="odd") for far in hidden),
            _mention(HUB_A, seen[0]),
        ])],
        edges=[
            *(_edge(hub, far) for hub in (HUB_A, HUB_B) for far in seen),
            *(_edge(HUB_A, far) for far in hidden),
        ],
    )
    _trust(clean_runtime, deployment, MAIN)
    _activate(clean_runtime, deployment, _DENY_EXTERNAL)
    monkeypatch.setattr(tools_module, "EXPAND_LINK_BUDGET", 3)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    calls: list[dict[str, Any]] = [
        {}, {"limit": 2}, {"limit": 2, "offset": 2}, {"direction": "both"},
        {"where": {"attrs.kind": "odd"}}, {"order_by": "node_id", "limit": 2},
        {"select": ["node_id", "attrs", "link.attrs"]},
    ]
    server = Tools(clean_runtime, deployment)
    try:
        for call in calls:
            pair = []
            for hub in (HUB_A, HUB_B):
                arguments = {"input": {"node_id": hub}, **call}
                reply = _served(_reply(server, arguments))
                assert reply["total_count_basis"] == "omitted_over_link_budget", (hub, call)
                assert reply["total_count"] is None and reply["truncated"] is True
                assert set(_ids(reply)) <= set(seen), (hub, call)
                pair.append(
                    _neutral(json.dumps(_same(reply), sort_keys=True), (HUB_A, HUB_B))
                    + "\n" + _neutral(server.text("expand", arguments), (HUB_A, HUB_B))
                )
            assert pair[0] == pair[1], call
            assert "hidden passage" not in pair[0] and "doc_chunk:x" not in pair[0], call
        whole = _served(_reply(server, {"input": {"node_id": HUB_A}}))
        # Failure case: within the budget the same hubs are counted exactly.
        monkeypatch.setattr(tools_module, "EXPAND_LINK_BUDGET", 6)
        exact = [_served(_reply(server, {"input": {"node_id": hub}})) for hub in (HUB_A, HUB_B)]
    finally:
        server.close()
    # The three records the first three links reach, the same for both.
    assert sorted(_ids(whole)) == sorted(seen[:3])
    assert "where, limit and offset work within those records" in json.dumps(whole)
    for reply in exact:
        assert reply["total_count"] == 6 and reply["total_count_basis"] == "exact_visible"
        assert sorted(_ids(reply)) == sorted(seen)


SCAN_FEW, SCAN_MANY, SCAN_EDGE = "doc:scan-few", "doc:scan-many", "doc:scan-edge"


def test_a_record_with_more_stored_links_than_the_scan_cap_is_refused_alike(
        clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    """Under access rules the link budget counts visible links only, so the
    stored links read are capped as well (`EXPAND_LINK_SCAN_CAP`). Two hubs
    each link to the same two passages the reader may see, and to six and
    to nine passages it may not. With the cap at four both reach it and are
    refused with the same fixed sentence, which says nothing about how many
    links are stored or hidden. A hub with exactly four stored links is
    served, and a cap above every hub's links serves all three."""
    from okg.substrate.mcp import tools as tools_module

    seen = ["doc_chunk:s0", "doc_chunk:s1"]
    hidden = {SCAN_FEW: 6, SCAN_MANY: 9, SCAN_EDGE: 2}
    hidden_ids = {hub: [f"doc_chunk:{hub[4:]}-x{n}" for n in range(count)]
                  for hub, count in hidden.items()}
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment, subtypes=[DOC, DOC_CHUNK],
        narrowings=[("doc", "contains", "doc_chunk")],
        nodes=[],
        node_groups=[(MAIN, [
            *(_node(hub, "doc", f"hub {hub}") for hub in hidden),
            *(_node(far, "doc_chunk", f"passage {far}", kind="odd") for far in seen),
        ]), (EXTERNAL, [
            _node(far, "doc_chunk", f"hidden passage {far}", kind="odd")
            for fars in hidden_ids.values() for far in fars
        ])],
        edges=[
            *(_edge(hub, far) for hub in hidden for far in seen),
            *(_edge(hub, far) for hub, fars in hidden_ids.items() for far in fars),
        ],
    )
    _trust(clean_runtime, deployment, MAIN)
    _activate(clean_runtime, deployment, _DENY_EXTERNAL)
    monkeypatch.setattr(tools_module, "EXPAND_LINK_SCAN_CAP", 4)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    server = Tools(clean_runtime, deployment)
    try:
        refusals = []
        for hub in (SCAN_FEW, SCAN_MANY):
            (result,) = server.run(("expand", {"input": {"node_id": hub}}, "json"))
            assert result.isError, hub
            error = json.loads(result.content[0].text)["error"]
            assert error["code"] == "expand_link_scan_bound", error
            assert error["message"] == tools_module.EXPAND_LINK_SCAN_BOUND_MESSAGE
            text = server.text("expand", {"input": {"node_id": hub}})
            refusals.append(_neutral(json.dumps(error, sort_keys=True) + "\n" + text,
                                     (SCAN_FEW, SCAN_MANY)))
        # Six hidden links and nine give the same refusal, word for word.
        assert refusals[0] == refusals[1]
        assert "hidden passage" not in refusals[0] and "-x" not in refusals[0]
        # Exactly at the cap with no stored link left: served, not refused.
        edge = _served(_reply(server, {"input": {"node_id": SCAN_EDGE}}))
        assert sorted(_ids(edge)) == seen and edge["total_count"] == 2
        # Failure case: a cap above every hub's stored links serves them all.
        monkeypatch.setattr(tools_module, "EXPAND_LINK_SCAN_CAP", 20)
        for hub in hidden:
            reply = _served(_reply(server, {"input": {"node_id": hub}}))
            assert sorted(_ids(reply)) == seen, hub
            assert reply["total_count"] == 2 and reply["total_count_basis"] == "exact_visible"
    finally:
        server.close()
