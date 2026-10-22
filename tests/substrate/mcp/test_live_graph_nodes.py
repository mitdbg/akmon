"""One live node and its live edges: `inspect` and `expand` (#3666).

PACT live-graph-rucio-sites: inspect, expand and `args`; `exists` outcomes
(a gate deny never says "checked live"); the keep rule.

Every call goes through the server's own `call_tool` (the lab of
`test_live_graph_keep.py`: the shared Example fixture, the real registry, a
recording fake Example), so each assertion counts the adapter calls a client's
call really made.
"""
from __future__ import annotations

import json
from typing import Any

import pytest

from okg.substrate.mcp import live_nodes
from tests.substrate.mcp.test_live_graph_keep import (  # noqa: F401
    AllowGate,
    clock,
    lab,
    site,
    unfence,
)

SITE = f"site:{site(1)}"

# `AllowGate`, `clock`, `lab` and `unfence` are pytest fixtures and helpers
# imported from test_live_graph_keep; the tests below take the fixtures by
# parameter name. Naming them here once keeps ruff from reading each parameter
# as a redefinition of an unused import.
_IMPORTED_FIXTURES = (AllowGate, clock, lab, unfence)


# ============================================================ hermetic


def test_args_is_hidden_from_the_published_expand_schema() -> None:
    from okg.substrate.mcp.lean_surface import HIDDEN_ARGUMENTS

    assert "args" in HIDDEN_ARGUMENTS["expand"]


def test_a_stored_call_is_not_a_live_node() -> None:
    class _Registry:
        types = {"site": object()}

        def by_prefix(self, prefix: str) -> Any:
            return None

    assert live_nodes.detect_node(_Registry(), "inspect", {
        "target": {"kind": "node", "node_id": "page:1"}}) is None
    assert live_nodes.detect_node(_Registry(), "expand", {"input": "page:1"}) is None
    assert live_nodes.detect_node(_Registry(), "inspect", {"target": {"kind": "graph"}}) is None


# ============================================================ inspect


@pytest.mark.postgres
def test_inspect_of_a_live_node_makes_exists_then_the_default_field_calls(lab) -> None:
    lab.example.set_sites(3)
    body = lab.ok("inspect", {"target": {"kind": "node", "node_id": SITE}})
    assert lab.example.made("example_get_site") == 1
    # static_total and static_used share one call per site.
    assert lab.example.made("example_get_site_usage_by_source") == 1
    assert lab.example.total() == 2
    assert body["node_id"] == SITE and body["subtype"] == "site"
    assert unfence(body["attrs"]["kind"]) == "HOT"
    assert body["attrs"]["static_total"] == 100
    assert body["live"]["system"] == "example"
    assert body["live"]["fields"]["static_total"] == "live"
    assert body["fields"]["static_total"].startswith("1 Example call per site")
    assert "attrs.<any other key>" in body["fields"]
    edges = {edge["edge_type"]: edge for edge in body["edges"]}
    assert edges["distance"]["args"] == {"to": "required"} and edges["holds"]["to"] == "item"
    assert body["live_line"].startswith("live · Example · read ")


@pytest.mark.postgres
def test_inspect_with_a_select_calls_only_for_the_fields_selected(lab) -> None:
    lab.example.set_sites(3)
    body = lab.ok("inspect", {"target": {"kind": "node", "node_id": SITE},
                              "select": ["attrs.tier"]})
    assert lab.example.made("example_get_site") == 1
    assert lab.example.made("example_list_site_attributes") == 1
    assert lab.example.made("example_get_site_usage_by_source") == 0
    assert unfence(body["attrs"]["tier"]) == "2"


@pytest.mark.postgres
def test_an_unknown_site_is_not_visible_and_a_failed_exists_is_example_unavailable(lab) -> None:
    lab.example.set_sites(3)
    missing = lab.refused("inspect", {"target": {"kind": "node", "node_id": "site:Z2_XX_NOPE"}})
    assert missing["code"] == "node_not_found"
    assert "not visible" in missing["message"]
    assert lab.example.total() == 1
    lab.example.fail_exists = {site(1)}
    down = lab.refused("inspect", {"target": {"kind": "node", "node_id": SITE}})
    assert down["code"] == "live_read_refused"
    assert down["data"]["reason"] == "system_unavailable"
    assert "Example unavailable" in down["message"]
    assert lab.example.made("example_get_site_usage_by_source") == 0


@pytest.mark.postgres
def test_a_gate_deny_reads_as_absence_and_never_says_checked_live(lab) -> None:
    lab.example.set_sites(3)
    lab.mcp._okg_live_gate = AllowGate(deny=("example_get_site",))
    denied = lab.refused("inspect", {"target": {"kind": "node", "node_id": SITE}})
    lab.mcp._okg_live_gate = AllowGate()
    absent = lab.refused("inspect", {"target": {"kind": "node", "node_id": "site:Z2_XX_NOPE"}})
    assert lab.example.made("example_get_site") == 1  # the absent one only
    assert denied["code"] == absent["code"] == "node_not_found"
    assert denied["message"].replace(SITE, "X") == absent["message"].replace("site:Z2_XX_NOPE", "X")
    assert "checked live" not in json.dumps(denied) and "live" not in denied["message"]


@pytest.mark.postgres
def test_a_malformed_live_id_is_refused_with_no_call(lab) -> None:
    error = lab.refused("inspect", {"target": {"kind": "node", "node_id": "site:not-a-site"}})
    assert error["code"] == "live_node_id_invalid"
    assert lab.example.total() == 0


@pytest.mark.postgres
def test_inspect_of_system_example_reads_its_declared_fields_and_lists_its_edges(lab) -> None:
    body = lab.ok("inspect", {"target": {"kind": "node", "node_id": "system:example"}})
    assert lab.example.made("example_whoami") == 1 and lab.example.made("example_ping") == 1
    assert lab.example.total() == 2
    assert unfence(body["attrs"]["whoami"]["account"]) == "jdoe"
    assert set(body["fields"]) == {"ping", "whoami"}
    assert [edge["edge_type"] for edge in body["edges"]] == ["sites"]


@pytest.mark.postgres
def test_inspect_of_a_live_subtype_gives_the_declaration_with_no_call(lab) -> None:
    body = lab.ok("inspect", {"target": {"kind": "subtype", "subtype": "site"}})
    assert lab.example.total() == 0
    assert body["live"] is True and body["system"] == "example" and body["calls_made"] == 0
    assert body["fields"]["static_total"].startswith("1 Example call per site")
    assert {edge["edge_type"] for edge in body["edges"]} == {"distance", "holds"}
    assert "system:example -> sites" in body["set"]


@pytest.mark.postgres
def test_a_stored_inspect_is_unchanged(lab) -> None:
    registry = lab.mcp._okg_live_types
    lab.mcp._okg_live_types = None
    before = lab.ok("inspect", {"target": {"kind": "node", "node_id": lab.hub}})
    lab.mcp._okg_live_types = registry
    after = lab.ok("inspect", {"target": {"kind": "node", "node_id": lab.hub}})
    assert _stable(after) == _stable(before)
    assert lab.example.total() == 0


def _stable(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            k: _stable(v) for k, v in value.items()
            if not k.endswith("_seconds") and k != "elapsed_ms"
        }
    if isinstance(value, list):
        return [_stable(v) for v in value]
    return value


@pytest.mark.postgres
def test_the_text_reply_of_a_live_inspect_renders(lab) -> None:
    lab.example.set_sites(3)
    result = lab.call("inspect", {"target": {"kind": "node", "node_id": SITE}}, "text")
    text = result.content[0].text
    assert result.isError is False, text[:2000]
    assert "live · Example" in text and SITE in text


# ============================================================ expand


@pytest.mark.postgres
def test_expand_system_example_sites_lists_the_sites_and_keeps_a_cut_reply(lab) -> None:
    lab.example.set_sites(30)
    body = lab.ok("expand", {"input": "system:example", "edge_type": "sites", "limit": 5})
    assert lab.example.calls == [("example_list_all_sites", {"limit": 1000})]
    assert [row["node_id"] for row in body["neighbors"]] == [f"site:{site(i)}" for i in range(5)]
    assert body["total_count"] == 30 and body["truncated"] is True
    ref = body["result_reference"]["id"]
    assert body["next_call"] == {"tool": "filter",
                                 "arguments": {"input": {"ref": ref}, "offset": 5}}
    assert f"kept as {ref}" in body["live_line"]
    page = lab.ok("filter", {"input": {"ref": ref}, "offset": 5, "limit": 5})
    assert [row["node_id"] for row in page["items"]] == [f"site:{site(i)}" for i in range(5, 10)]
    assert lab.example.total() == 1


@pytest.mark.postgres
def test_expand_a_site_s_locks_returns_far_end_typed_nodes(lab) -> None:
    lab.example.set_sites(3)
    body = lab.ok("expand", {"input": {"node_id": SITE}, "edge_type": "holds"})
    assert lab.example.args_of("example_list_site_items") == [{"site": site(1), "limit": 1000}]
    # `item` declares no name pattern: its ids are upstream names, fenced.
    assert [unfence(row["node_id"]) for row in body["neighbors"]] == [
        f"item:grp:/p/q/r#{i}" for i in range(3)
    ]
    assert {row["subtype"] for row in body["neighbors"]} == {"item"}
    assert "result_reference" not in body


@pytest.mark.postgres
def test_an_arg_given_as_a_node_id_reaches_the_relation_as_its_name(lab) -> None:
    lab.example.set_sites(3)
    other = f"site:{site(2)}"
    body = lab.ok("expand", {"input": SITE, "edge_type": "distance", "args": {"to": other}})
    assert lab.example.args_of("example_get_link") == [
        {"source_site": site(1), "destination_site": site(2)},
    ]
    (row,) = body["neighbors"]
    assert row["node_id"] == other and row["attrs"]["distance"] == 3


@pytest.mark.postgres
@pytest.mark.parametrize("arguments,code,reason", [
    ({"edge_type": "distance"}, "live_arg_missing", None),
    ({"edge_type": "distance", "args": {"to": "item:grp:/p/q/r"}}, "live_node_id_invalid", None),
    ({"edge_type": "distanse"}, "live_read_refused", "unknown_edge"),
    ({"edge_type": "holds", "args": {"to": "x"}}, "live_read_refused", "unknown_edge_argument"),
    ({}, "live_read_refused", "edge_type_required"),
    ({"edge_type": "holds", "direction": "in"}, "live_read_refused", "argument_not_live"),
    ({"edge_type": "holds", "generation_id": 1}, "live_read_refused",
     "generation_id_on_live_type"),
])
def test_every_refusal_of_a_live_expand_makes_no_call(lab, arguments, code, reason) -> None:
    lab.example.set_sites(3)
    error = lab.refused("expand", {"input": SITE, **arguments})
    assert error["code"] == code, error
    if reason is not None:
        assert error["data"]["reason"] == reason
    assert lab.example.total() == 0


@pytest.mark.postgres
def test_args_on_a_stored_node_is_refused(lab) -> None:
    error = lab.refused("expand", {"input": lab.hub, "edge_type": "cites", "args": {"to": "x"}})
    assert error["data"]["argument"] == "args"


@pytest.mark.postgres
def test_a_where_on_the_sites_edge_is_applied_by_example(lab) -> None:
    lab.example.set_sites(10)
    lab.example.expressions["kind=COLD"] = [site(0), site(5)]
    body = lab.ok("expand", {"input": "system:example", "edge_type": "sites",
                             "where": {"attrs.kind": "COLD"}})
    assert lab.example.calls == [("example_list_sites", {"site_expression": "kind=COLD",
                                                    "limit": 1000})]
    assert [row["node_id"] for row in body["neighbors"]] == [f"site:{site(0)}", f"site:{site(5)}"]
    assert "applied by Example: kind=COLD" in body["live_line"]


@pytest.mark.postgres
def test_a_partly_pushed_where_on_sites_joins_the_listing_for_the_rest(lab) -> None:
    """`kind` is pushed; the pushed rows carry `site` only, so the rest
    (`availability`) is evaluated on the full listing's columns, joined with
    one call, never read as absent on every row."""
    lab.example.set_sites(10)
    lab.example.sites[5]["availability"] = 1
    lab.example.expressions["kind=COLD"] = [site(0), site(5)]
    body = lab.ok("expand", {"input": "system:example", "edge_type": "sites", "where": {
        "op": "and", "clauses": [
            {"op": "eq", "field": "attrs.kind", "value": "COLD"},
            {"op": "gt", "field": "attrs.availability", "value": 3},
        ]}})
    assert [name for name, _ in lab.example.calls] == ["example_list_sites", "example_list_all_sites"]
    assert [row["node_id"] for row in body["neighbors"]] == [f"site:{site(0)}"]
    (row,) = body["neighbors"]
    assert unfence(row["attrs"]["kind"]) == "COLD"
    assert "applied by Example: kind=COLD" in body["live_line"]


# ============================================================ the typo rule on a kept expand


@pytest.mark.postgres
def test_a_sparse_key_over_a_narrowed_expand_is_noted_and_a_typo_over_a_whole_one_refused(
    lab,
) -> None:
    """An expand Example narrowed (`kind=COLD`) keeps a narrowed set: a
    key none of its rows carries may be real, so it is `(absent)` with a
    note. Over the whole listing the same kind of key is a probable typo."""
    lab.example.set_sites(10)
    lab.example.expressions["kind=COLD"] = [site(0), site(5)]
    narrowed = lab.ok("expand", {"input": "system:example", "edge_type": "sites",
                                 "where": {"attrs.kind": "COLD"}, "limit": 1})
    ref = narrowed["result_reference"]["id"]
    body = lab.ok("aggregate", {"input": {"ref": ref}, "by": ["attrs.disk_only"],
                                "metrics": ["count"]})
    counts = {unfence(g["attrs.disk_only"]): g["count"] for g in body["groups"]}
    assert counts == {"(absent)": 2}, body["groups"]
    note = body["keys_absent"]
    assert "attrs.disk_only" in note["keys"], note
    assert "no row in this set carries attrs.disk_only" in note["note"]

    whole = lab.ok("expand", {"input": "system:example", "edge_type": "sites", "limit": 5})
    error = lab.refused("aggregate", {"input": {"ref": whole["result_reference"]["id"]},
                                      "by": ["attrs.keep_fag"], "metrics": ["count"]})
    assert error["data"]["reason"] == "key_absent_on_every_row", error
    assert "keep_flag" in error["data"]["closest"]
