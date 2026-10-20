"""Step-3 gate for console-count-rollup (#444 slice 2): the dirty scope detects a
node SOURCE reattribution.

A node's by-source count bucket moves when its dedicated ``source`` column changes
on an upsert, but the node id is unchanged so nothing else flags it. The count
rollup forces a full recount on this marker. It is stamped on the UPSERT path only;
a DELETE's source -1 is fold-owned, so the delete path must NOT stamp it (stamping
both would double-count the move).
"""
from __future__ import annotations

import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact

pytestmark = pytest.mark.postgres


def _node(node_id: str, subtype: str, text: str, *, rev: int) -> NodeFact:
    return NodeFact(node_id, subtype, {"text": text}, {"id": node_id}, {"v": rev})


def test_source_reattribution_stamps_source_marker(clean_runtime: str) -> None:
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts([_node("n:1", "claim", "base", rev=1)], source="srcA")
        # Same subtype + same attrs, NEW source: only the source moved.
        result = store.apply_facts([_node("n:1", "claim", "base", rev=2)], source="srcB")
        markers = result.node_attr_changes_by_subtype["claim"]["n:1"]
        assert "__source__" in markers, markers
        assert "__subtype__" not in markers, markers
    finally:
        store.close()


def test_attr_change_without_source_change_has_no_source_marker(
    clean_runtime: str,
) -> None:
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts([_node("n:1", "claim", "base", rev=1)], source="srcA")
        result = store.apply_facts([_node("n:1", "claim", "changed", rev=2)], source="srcA")
        markers = result.node_attr_changes_by_subtype["claim"]["n:1"]
        assert "text" in markers, markers
        assert "__source__" not in markers, markers
    finally:
        store.close()


def test_node_delete_has_no_source_marker(clean_runtime: str) -> None:
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts([_node("n:1", "claim", "base", rev=1)], source="srcA")
        result = store.apply_facts(
            [NodeFact("n:1", "claim", {}, {"id": "n:1"}, {"v": 2}, op="R")],
            source="srcA",
        )
        markers = result.node_attr_changes_by_subtype["claim"]["n:1"]
        assert "__deleted__" in markers, markers
        assert "__source__" not in markers, markers
    finally:
        store.close()


def test_new_node_marker_is_new_not_source(clean_runtime: str) -> None:
    """A brand-new node is __new__ (the by-source +1 is the fold's add leg, not a
    reattribution); it must not carry the reattribution force marker."""
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        result = store.apply_facts([_node("n:9", "claim", "fresh", rev=1)], source="srcA")
        markers = result.node_attr_changes_by_subtype["claim"]["n:9"]
        assert "__new__" in markers, markers
        assert "__source__" not in markers, markers
    finally:
        store.close()
