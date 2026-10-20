"""Set-input validation (operator-surface-list-work req.listwork.set-operators).

Since #3201 the set reads (ordered filter, set aggregate, filtered one-hop
expand) run on the effective-attribute SQL relation in server.py and are
covered against real publishes in test_operator_parity_db.py,
test_pagination_truth.py and test_expand_pagination_contract.py. What stays
here is the pure grammar: which inputs count as set inputs, and which scope
narrowings are refused before any SQL runs.
"""

from __future__ import annotations

import pytest

from okg.substrate.mcp.set_operators import is_set_input, validate_set_scope


def test_is_set_input_shapes() -> None:
    assert is_set_input({"subtype": "pact_change"})
    assert is_set_input({"kind": "edges", "edge_type": "supersedes"})
    assert is_set_input(
        {"filter": {"op": "eq", "field": "attrs.status", "value": "open"}}
    )
    assert not is_set_input({"node_id": "pact-change:x"})
    assert not is_set_input({"start_node": "pact-change:x"})
    assert not is_set_input("pact-change:x")
    assert not is_set_input({})


def test_scope_refuses_subtype_on_edges_and_edge_type_on_nodes() -> None:
    with pytest.raises(ValueError, match="subtype applies to kind='nodes'"):
        validate_set_scope("edges", {"kind": "edges", "subtype": "pact_change"})
    with pytest.raises(ValueError, match="edge_type applies to kind='edges'"):
        validate_set_scope("nodes", {"edge_type": "supersedes"})


def test_scope_accepts_matching_narrowings() -> None:
    validate_set_scope("nodes", {"kind": "nodes", "subtype": "page"})
    validate_set_scope("edges", {"kind": "edges", "edge_type": "cites"})
    validate_set_scope("nodes", {"kind": "nodes"})
    validate_set_scope("nodes", "not-a-mapping")
