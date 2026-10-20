"""inspect(graph)'s text prints each count map once (#2674).

The minimal schema digest rebuilds the page's own `subtype_counts` and
`edge_type_counts`, so the text printed subtype counts twice and edge counts
three times. The text view now drops a schema key only when it equals what
the page already carries; the JSON form is unchanged. No database.
"""
from __future__ import annotations

import copy

from okg.substrate.mcp.result_text import render
from okg.substrate.mcp.schema_digest import minimal_digest

SUBTYPES = {"jira_issue": 120, "jira_chunk": 400, "git_commit": 55}
EDGES = {"has_chunk": 400, "references": 30}


def _payload(**schema_overrides) -> dict:
    schema = minimal_digest(
        deployment_name="d", pinned_generation_id="gen:7",
        subtype_counts=SUBTYPES, edge_type_counts=EDGES,
        minimal_reason="Chronos-backed graph read path",
    )
    schema.update(schema_overrides)
    return {
        # The real payload's names (tools.inspect_graph).
        "status": "ok", "operator_name": "inspect", "deployment_name": "d",
        "generation_id": "gen:7",
        "subtype_counts": SUBTYPES, "subtype_count": 3, "node_count": 575,
        "edge_type_counts": EDGES, "edge_type_count": 2, "edge_count": 430,
        "schema": schema,
    }


def test_each_count_map_prints_once() -> None:
    payload = _payload()
    before = copy.deepcopy(payload)
    text = render(payload, tool="inspect", kind="graph")

    assert text.count('"jira_chunk"') == 1, text
    assert text.count('"references"') == 1, text
    # What only the schema says still prints.
    assert "score_functions" in text
    assert "minimal_reason" in text
    schema_text = text.split("schema:", 1)[1]
    assert "deployment_name: d" in text
    assert '"deployment"' not in schema_text, text
    assert "gen:7" not in schema_text, text
    assert payload == before, "the JSON payload is not changed"


def test_a_schema_that_differs_from_the_page_keeps_its_own_values() -> None:
    text = render(
        _payload(edge_type_counts={"has_chunk": 1}), tool="inspect", kind="graph",
    )
    # The page's map and the schema's own, different, counts both print; the
    # edge-type names still match the page, so they are dropped.
    assert text.count('"has_chunk"') == 2, text
    assert '"edge_type_counts":{"has_chunk":1}' in text
