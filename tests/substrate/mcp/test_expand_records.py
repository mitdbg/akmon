"""`expand` returns the linked records (issue #3666): the parts with no database.

Which 1-hop shape a `select` asks for, the one vocabulary both shapes are
registered in, how a served record becomes a row, what the page says above
the link budget, and how a record row prints. The served behaviour is in
test_expand_records_db.py.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from okg.substrate.mcp import tools
from okg.substrate.mcp.read_grammar import rewrite_call
from okg.substrate.mcp.result_text import render
from okg.substrate.mcp.select_vocabulary import (
    call_shape,
    plan_select,
    served_as_selected,
    shape_fields,
    vocabulary,
)

LINK_NAMES = ["edge_id", "src", "dst", "edge_type", "direction"]


@pytest.mark.parametrize("select", [
    LINK_NAMES,
    ["dst"],
    ["edge_id", "read_provenance"],
    # The link's own attributes beside one of its names: still a link row.
    ["edge_id", "link.attrs"],
    # The old spelling: beside a link field, `attrs` is the link's own.
    [*LINK_NAMES, "attrs"],
    ["dst", "attrs"],
])
def test_a_select_of_only_the_links_old_names_asks_for_link_rows(select):
    assert tools.expand_select_is_links(select)
    assert call_shape("expand", {"input": "n:1", "select": select}) == "edges"
    assert served_as_selected("expand", {"input": "n:1", "select": select})


@pytest.mark.parametrize("select", [
    None,
    [],
    ["node_id"],
    ["attrs"],
    # One record field beside the link's names: the rows are records.
    [*LINK_NAMES, "node_id"],
    ["node_id", "edge_type", "src", "dst", "attrs"],
    ["link"],
    ["link.attrs"],
    ["source_lineage"],
    "edge_id",
    ["edge_id", 3],
])
def test_any_other_select_asks_for_the_linked_records(select):
    assert not tools.expand_select_is_links(select)
    assert call_shape("expand", {"input": "n:1", "select": select}) == "records"


def test_the_shape_of_a_path_read_is_not_the_selects_to_choose():
    for arguments in ({"path": ["e:out"]}, {"depth": 2}):
        assert call_shape("expand", {"input": "n:1", **arguments, "select": LINK_NAMES}) == "path"
    assert call_shape("expand", {"next": "token"}) is None
    assert call_shape("expand", {"input": "n:1", "depth": "2"}) is None


def test_both_one_hop_shapes_are_in_the_one_vocabulary():
    words = vocabulary("expand")
    assert set(shape_fields("expand")) == {"records", "edges", "path"}
    # A record's fields are the same on a 1-hop row and on a path row.
    assert words["attrs"] == ("records", "edges", "path")
    for name in ("node_id", "subtype", "provenance", "source_lineage"):
        assert words[name] == ("records", "path"), name
    for name in ("link", "link_count", "title"):
        assert words[name] == ("records",), name
    for name in LINK_NAMES:
        assert words[name] == ("records", "edges"), name
    assert words["link.attrs"] == ("records", "edges")
    # The link row's own trust label is a field of the link view only.
    assert words["read_provenance"] == ("edges",)
    # `attrs` alone is the record's; it is the link's only beside a link field.
    assert not tools.expand_select_is_links(["attrs"])
    assert not tools.expand_select_is_links(["attrs", "link.attrs"])
    assert "neighbor" not in words and "attrs.status" not in words


def test_a_path_select_naming_the_link_is_served_with_a_note():
    plan = plan_select("expand", {
        "input": "n:1", "path": ["e:out"], "select": ["node_id", "link"],
    })
    assert plan.select == ("node_id",)
    assert plan.notes[0]["served"] == (
        "not returned, path rows do not return it (call a 1-hop expand for it)"
    )


def _link(edge_id: int, src: str, dst: str, *, direction: str = "out", edge_type: str = "contains"):
    return {
        "edge_id": edge_id, "src": src, "dst": dst, "edge_type": edge_type,
        "direction": direction, "attrs": {"role": f"r{edge_id}"},
    }


RECORDS = [
    {"node_id": "c:1", "subtype": "chunk", "attrs": {"text": "one"},
     "provenance": {"trust": "trusted"}, "text_hash": "h1"},
    {"node_id": "c:2", "subtype": "chunk", "attrs": {"text": "two"},
     "provenance": {"trust": "trusted"}, "text_hash": "h2"},
]
LINKS = {
    "c:1": [_link(7, "d:1", "c:1")],
    # In `expand_far_links` order: link type first, so `contains` leads.
    "c:2": [_link(9, "d:1", "c:2"), _link(3, "d:1", "c:2", edge_type="mentions")],
    # A far end with no served record has no row.
    "c:gone": [_link(4, "d:1", "c:gone")],
}
TITLES = {"c:1": {"subtype": "chunk", "title": "First"}}


def _rows(select):
    return tools.expand_record_rows(
        RECORDS, LINKS, select, titles=TITLES,
        link_attrs={7: {"attrs": {"role": "r7"}, "read_provenance": {"trust": "trusted"}},
                    9: {"attrs": {"role": "r9"}, "read_provenance": {"trust": "trusted"}}},
    )


def test_a_default_row_is_the_record_and_the_link_that_reached_it():
    assert _rows(None) == [
        {"node_id": "c:1", "subtype": "chunk", "title": "First",
         "link": {"edge_type": "contains", "direction": "out", "edge_id": 7}, "link_count": 1},
        # Two links reach c:2: it is one row, the first link is shown, and
        # the count says there are two.
        {"node_id": "c:2", "subtype": "chunk", "title": None,
         "link": {"edge_type": "contains", "direction": "out", "edge_id": 9}, "link_count": 2},
    ]


def test_every_row_has_the_same_keys_whatever_is_selected():
    for select in (None, ["node_id"], ["attrs"], ["node_id", "provenance"],
                   ["attrs", "link.attrs", "src", "dst"]):
        keys = {frozenset(row) for row in _rows(select)}
        assert len(keys) == 1, select
        assert set(tools.EXPAND_RECORD_ALWAYS) <= set(next(iter(keys)))


def test_attrs_are_the_records_and_the_links_are_under_link():
    (first, second) = _rows(["attrs", "link.attrs", "src", "dst"])
    # The record's attributes, with the trust label that travels with them.
    assert first["attrs"] == {"text": "one"} and first["provenance"] == {"trust": "trusted"}
    assert first["link"] == {
        "edge_type": "contains", "direction": "out", "edge_id": 7,
        "src": "d:1", "dst": "c:1",
        "attrs": {"role": "r7"}, "read_provenance": {"trust": "trusted"},
    }
    assert second["link"]["attrs"] == {"role": "r9"}
    # Nothing unselected: no attributes, no link ends, no stored-row keys.
    (plain, _) = _rows(["node_id"])
    assert "attrs" not in plain and "provenance" not in plain and "text_hash" not in plain
    assert set(plain["link"]) == {"edge_type", "direction", "edge_id"}


def test_link_counts_counted_in_sql_are_the_rows_link_counts():
    """With access rules off the links are grouped in SQL: `links` then
    holds only the shown link, and the count comes beside it."""
    shown = {far: reached[:1] for far, reached in LINKS.items()}
    rows = tools.expand_record_rows(
        RECORDS, shown, None, titles=TITLES, link_attrs={},
        link_counts={"c:1": 1, "c:2": 7000},
    )
    assert [row["link_count"] for row in rows] == [1, 7000]
    assert rows == [{**row, "link_count": count}
                    for row, count in zip(_rows(None), (1, 7000))]
    # Failure case: a record the SQL count does not know is not served a guess.
    with pytest.raises(KeyError):
        tools.expand_record_rows(
            RECORDS, shown, None, titles=TITLES, link_attrs={}, link_counts={"c:1": 1},
        )


def test_the_anchor_sql_names_every_direction_and_refuses_any_other():
    out, params = tools.expand_anchor_sql("d:1", None, "out")
    assert out == "e.src = :focal" and params == {"focal": "d:1"}
    both, params = tools.expand_anchor_sql("d:1", "contains", "both")
    assert both == "(e.src = :focal OR e.dst = :focal) AND e.edge_type = :etype"
    assert params == {"focal": "d:1", "etype": "contains"}
    # An empty type is a type, as on a link row: it is not "no type".
    assert tools.expand_anchor_sql("d:1", "", "in")[1] == {"focal": "d:1", "etype": ""}
    with pytest.raises(Exception) as refused:
        tools.expand_anchor_sql("d:1", None, "sideways")
    assert "direction must be one of" in str(refused.value)


def test_the_per_row_link_read_refuses_a_caller_that_read_the_rules_as_off():
    """With access rules off the far records are read in SQL; the per-row
    read refuses rather than serve rows it did not check. It refuses before
    it touches the connection or the session."""
    with pytest.raises(ValueError) as refused:
        tools.expand_far_links(None, None, "d:1", runtime_required=False)
    assert "serves audit and enforce only" in str(refused.value)


def test_an_ok_lineage_leaves_link_attrs_as_it_leaves_a_link_rows_attrs():
    """The wire form drops an `ok` source lineage from a link row's `attrs`.
    The same link's attributes on a record row, under `link`, are the same."""
    from okg.substrate.mcp.response_shape import compact_for_wire

    ok = {"nomos_source_lineage": {"status": "ok"}}
    bad = {"nomos_source_lineage": {"status": "missing"}}

    def wire(rows):
        payload = {"operator_name": "expand", "neighbors": rows}
        return compact_for_wire(payload, {})["neighbors"]

    (link_row,) = wire([{"edge_id": 7, "attrs": {"role": "cites", **ok}}])
    (record_row,) = wire([{
        "node_id": "c:1", "attrs": {"kind": "odd", **ok},
        "link": {"edge_id": 7, "edge_type": "mentions", "direction": "out",
                 "attrs": {"role": "cites", **ok}},
    }])
    assert link_row["attrs"] == record_row["link"]["attrs"] == {"role": "cites"}
    assert record_row["attrs"] == {"kind": "odd"}
    assert record_row["link"]["edge_id"] == 7
    # Failure case: a lineage that is not `ok` is kept, in both places.
    (kept,) = wire([{"node_id": "c:1", "link": {"edge_id": 7, "attrs": {"role": "x", **bad}}}])
    assert kept["link"]["attrs"] == {"role": "x", **bad}
    # A row with no link attributes is untouched.
    plain = {"node_id": "c:1", "link": {"edge_id": 7}}
    assert wire([plain]) == [plain]


def test_the_record_select_is_checked_against_the_record_fields():
    from okg.substrate.mcp.envelope import SelectUnknownField, validate_select

    validate_select(list(tools.EXPAND_RECORD_FIELDS), tools.EXPAND_RECORD_FIELDS, tool_name="expand")
    # Failure case: a dotted attribute and the old `neighbor` are refused.
    for name in ("attrs.text", "neighbor", "read_provenance"):
        with pytest.raises(SelectUnknownField):
            validate_select([name], tools.EXPAND_RECORD_FIELDS, tool_name="expand")


SESSION = SimpleNamespace(generation_id="gen:1")


def _page(**changes):
    arguments = {
        "direction": "out", "rows": _rows(None), "limit": 2, "offset": 0, "has_more": False,
        "over_budget": False,
        "count_fields": {"total_count": 2, "total_count_basis": "exact",
                         "retrieval_exhaustive": True},
    }
    return tools.expand_records_page(SESSION, "d:1", **{**arguments, **changes})


def test_a_page_counts_records_and_continues_by_records():
    whole = _page()
    assert whole["total_count"] == 2 and whole["total_count_basis"] == "exact"
    assert whole["truncated"] is False and whole["next_offset"] is None
    assert whole["returned_count"] == 2 and whole["neighbors"] == _rows(None)
    cut = _page(has_more=True, offset=4,
                count_fields={"total_count": 9, "total_count_basis": "exact"})
    assert cut["truncated"] is True and cut["next_offset"] == 6


def test_above_the_link_budget_no_total_is_given_and_the_page_is_never_complete():
    more = _page(over_budget=True, has_more=True)
    assert more["total_count"] is None
    assert more["total_count_basis"] == tools.OVER_LINK_BUDGET == "omitted_over_link_budget"
    assert more["truncated"] is True and more["next_offset"] == 2
    assert "retrieval_exhaustive" not in more and "5,000 links you may read" in more["note"]
    # The known records ran out: still not complete, no cursor, and why.
    last = _page(over_budget=True, has_more=False)
    assert last["total_count"] is None and last["truncated"] is True
    assert last["next_offset"] is None and last["pagination_capped"] is True
    # The notice names what works: paging within, and how to reach the rest.
    notice = last["pagination_notice"]
    assert "where, limit and offset work within those records" in notice
    assert "narrow edge_type or direction" in notice and "pages every link" in notice
    # Failure case: within the budget none of this is said.
    assert not {"note", "pagination_capped", "pagination_notice"} & set(_page())


def test_a_record_row_prints_the_record_then_the_link():
    page = dict(_page(rows=_rows(["attrs"])), operator_name="expand", row_trust="trusted")
    text = render(page, tool="expand")
    (first,) = [line for line in text.splitlines() if line.startswith("[1] ")]
    assert first.startswith('[1] c:1 (chunk) "First" · via contains → · edge 7')
    (second,) = [line for line in text.splitlines() if line.startswith("[2] ")]
    assert second.startswith("[2] c:2 (chunk) · via contains → · edge 9 · 2 links")
    # The record's attributes are key lines under its row.
    body = text[text.index("[1] "):]
    assert "text" in body and "one" in body and "two" in body
    incoming = _rows(None)
    incoming[0]["link"]["direction"] = "in"
    text = render(dict(_page(rows=incoming), operator_name="expand"), tool="expand")
    assert "[1] c:1 (chunk) \"First\" · via contains ← · edge 7" in text


def _replayed() -> list[dict]:
    data = json.loads(
        (Path(__file__).parent / "data" / "select_refusals_3666.json").read_text()
    )
    return [row for row in data["rows"] if row["tool"] == "expand"]


def test_the_replayed_refused_calls_all_select_fields_a_record_row_has():
    """The 21 `expand` calls agents made and the server refused (13 distinct
    shapes, values replaced by neutral ones): each selects a record's
    fields on a 1-hop read. The rewrite takes every one as written."""
    rows = _replayed()
    assert sum(row["calls"] for row in rows) == 21 and len(rows) == 13
    for row in rows:
        arguments = row["arguments"]
        assert "node_id" in arguments["select"]
        call = rewrite_call("expand", arguments)
        assert call.arguments == arguments and call.adjusted == ()
        assert call_shape("expand", arguments) == "records"
        assert served_as_selected("expand", arguments)
