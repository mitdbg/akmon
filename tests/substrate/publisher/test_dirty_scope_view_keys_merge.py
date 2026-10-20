"""Database-free checks for issue #2941: the dirty scope after the view
writes is the scope read before them with the view keys read again."""

from __future__ import annotations

from okg.substrate.publisher.publisher import (
    _dirty_scope_normalized,
    _merge_keyed_dirty_scope,
)


def _scope(nodes, edges, changes):
    return (nodes, edges, True, changes, {}, None)


def test_view_keys_replace_their_entries_and_leave_the_rest() -> None:
    before = _scope(
        {"doc": ["n:1", "n:2"], "view": ["view:a"]},
        {"cites": [1, 2]},
        {
            "doc": {"n:1": ["text"], "n:2": ["__new__"]},
            "view": {"view:a": ["rows"]},
        },
    )
    # The view rewrote view:a (now with another changed field), added
    # view:b, wrote edge 3, and put edge 2 back to its previous state, so
    # the keyed read no longer returns it.
    keyed = _scope(
        {"view": ["view:a", "view:b"]},
        {"cites": [3]},
        {"view": {"view:a": ["rows", "title"], "view:b": ["__new__"]}},
    )
    merged = _merge_keyed_dirty_scope(
        before, keyed,
        node_ids=["view:a", "view:b"], edge_ids=[2, 3],
    )
    assert merged is not None
    assert _dirty_scope_normalized(merged) == _dirty_scope_normalized(_scope(
        {"doc": ["n:1", "n:2"], "view": ["view:a", "view:b"]},
        {"cites": [1, 3]},
        {
            "doc": {"n:1": ["text"], "n:2": ["__new__"]},
            "view": {"view:a": ["rows", "title"], "view:b": ["__new__"]},
        },
    ))


def test_a_key_whose_subtype_changed_moves_and_empty_lists_drop() -> None:
    before = _scope({"old": ["n:1"]}, {}, {"old": {"n:1": ["text"]}})
    keyed = _scope({"new": ["n:1"]}, {}, {"new": {"n:1": ["__subtype__"]}})
    merged = _merge_keyed_dirty_scope(before, keyed, node_ids=["n:1"], edge_ids=[])
    assert merged is not None
    assert merged[0] == {"new": ["n:1"]}
    assert merged[3] == {"new": {"n:1": ["__subtype__"]}}


def test_a_scope_with_parts_a_scan_does_not_fill_is_not_merged() -> None:
    plain = _scope({}, {}, {})
    hinted = ({}, {}, True, {}, {"cites": [{"edge_id": 1}]}, None)
    with_edge_changes = ({}, {}, True, {}, {}, {"cites": {1: ["w"]}})
    no_changes = ({}, {}, True, None, {}, None)
    for scope in (hinted, with_edge_changes, no_changes):
        assert _merge_keyed_dirty_scope(scope, plain, node_ids=[], edge_ids=[]) is None
        assert _merge_keyed_dirty_scope(plain, scope, node_ids=[], edge_ids=[]) is None
    no_edge_table = ({}, {}, False, {}, {}, None)
    assert _merge_keyed_dirty_scope(plain, no_edge_table, node_ids=[], edge_ids=[]) is None
