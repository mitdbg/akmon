"""`map` answers over exactly the rows its `input` selects, under every
projection (issue #3476, PACT map-scope-3476).

Before the fix, a projection that read no attribute lowered a TransformProgram
with no WHERE: `input.subtype`, `input.edge_type` and an attribute-free
`input.filter` were dropped (the fixture holds one edge type, so the edge
case shares the node cases' code path rather than its own test) and every node or edge came back, with page facts
that looked complete. Each case below compares `map` with `filter` over the
same rows, selected by `filter`'s predicate.
"""
from __future__ import annotations

import pytest

from tests.substrate.mcp.test_search_records import _basic_facts, _publish


@pytest.fixture
def tools(clean_runtime, okg_mcp_can_log_in, tmp_path):
    from okg.substrate.mcp import server as srv

    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_basic_facts())
    mcp = srv.build_server(dsn=clean_runtime, deployment_name=deployment)
    try:
        yield {name: tool.fn for name, tool in mcp._tool_manager._tools.items()}
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def _ids(payload: dict, field: str = "node_id") -> list:
    assert payload.get("status") != "refused", payload
    return sorted(str(item[field]) for item in payload["items"])


@pytest.mark.postgres
@pytest.mark.parametrize("projection", [["node_id", "subtype"], ["node_id"], {"id": "node_id"}])
def test_map_keeps_the_subtype_scope(tools, projection):
    scope = {"kind": "nodes", "subtype": "comment"}
    expected = _ids(tools["filter"](
        input={"kind": "nodes"},
        predicate={"op": "eq", "field": "subtype", "value": "comment"}, limit=200))
    mapped = tools["map"](input=scope, projection=projection, limit=200)
    field = "id" if isinstance(projection, dict) else "node_id"
    assert expected, "precondition: the fixture holds comments"
    assert _ids(mapped, field) == expected
    assert all(str(i).startswith("comment:") for i in _ids(mapped, field))
    assert mapped["truncated"] is False


@pytest.mark.postgres
def test_map_keeps_an_attribute_free_filter(tools):
    clause = {"op": "eq", "field": "subtype", "value": "ticket"}
    scope = {"kind": "nodes", "filter": clause}
    expected = _ids(tools["filter"](input={"kind": "nodes"}, predicate=clause, limit=200))
    mapped = tools["map"](input=scope, projection=["node_id", "subtype"], limit=200)
    assert expected and _ids(mapped) == expected


@pytest.mark.postgres
def test_map_over_a_result_reference_keeps_its_members(tools):
    """A result reference names its members; an attribute-free projection
    over it returns those members and nothing else."""
    page = tools["search"](query="zirconium cladding", method="lexical", limit=5,
                           retain=True)
    assert page.get("status") != "refused", page
    ref = page["result_reference"]["id"]
    # The reference holds records only (passages on the page are left out),
    # so its members are read with `filter` over the same reference.
    shown = {str(row["subtype"]) for row in page["results"]}
    members = _ids(tools["filter"](
        input={"ref": ref},
        predicate={"op": "or", "clauses": [
            {"op": "eq", "field": "subtype", "value": subtype} for subtype in sorted(shown)
        ]}, limit=200))
    mapped = tools["map"](input={"ref": ref}, projection=["node_id"], limit=200)
    assert members and _ids(mapped) == members
    assert set(members) <= {str(row["node_id"]) for row in page["results"]}


@pytest.mark.postgres
def test_an_unscoped_attribute_free_map_is_unchanged(tools):
    """No narrowing in the input: the projection-only read still serves it."""
    everything = tools["map"](input={"kind": "nodes"}, projection=["node_id"], limit=200)
    subtypes = {item["node_id"].split(":", 1)[0] for item in everything["items"]}
    assert {"ticket", "comment"} <= subtypes


@pytest.mark.postgres
def test_an_integer_edge_id_filter_finds_its_edge(tools):
    """`map` serves edge_id as an integer; passing it back in a filter must
    find that edge, not an empty page marked complete."""
    edges = tools["map"](input={"kind": "edges"}, projection=["edge_id"], limit=5)
    edge_id = edges["items"][0]["edge_id"]
    assert isinstance(edge_id, int)
    mapped = tools["map"](
        input={"kind": "edges", "filter": {"op": "eq", "field": "edge_id", "value": edge_id}},
        projection=["edge_id", "src", "dst"], limit=10,
    )
    assert [item["edge_id"] for item in mapped["items"]] == [edge_id]


@pytest.mark.postgres
@pytest.mark.parametrize("scope", [
    {"kind": "nodes", "subtype": "comment"},
    {"kind": "nodes"},
])
def test_an_unknown_projection_field_is_refused_scoped_or_not(tools, scope):
    """Before the fix only the unscoped call reached the field check; the
    scoped call must refuse the same unknown field, not serve nulls."""
    page = tools["map"](input=scope, projection=["node_id", "text"], limit=5)
    assert page.get("status") == "refused", page
    assert page["error"]["code"] == "unsupported_operator_form"
