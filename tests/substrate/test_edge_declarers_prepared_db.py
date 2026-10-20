"""PostgreSQL proof for issue #2941 step D1: ranked edges decided before the
publish lock.

An edge-only apply prepared ahead (``key_fingerprint=True``) now also reads
the touched ranked edges' stored rows and declarations and reconciles them
before the lock. Under the lock a verified chunk only writes that decision.
These tests show that:

- a verified chunk reuses its decision, and stores what the direct path
  stores;
- a declaration recorded between the preparation and the write, with no
  graph row changed, changes the key fingerprint, so the write decides again
  and hands the edge to the new declarer instead of deleting it.
"""

from __future__ import annotations

from typing import Any

import pytest

from tests.substrate.test_edge_declarers_db import (
    ENRICHER,
    RANKING,
    STRUCTURE,
    _edge,
    _node,
    _rank,
)

pytestmark = pytest.mark.postgres

_EDGES = (
    "SELECT edge_id, src, dst, edge_type, attrs, source, source_record_id, "
    "source_revision FROM okg.graph_edges WHERE edge_type = 'calls' "
    "ORDER BY edge_id"
)
_DECLARERS = (
    "SELECT edge_id, source, src, dst, edge_type, attrs, source_record_id, "
    "source_revision FROM okg.graph_edge_declarers ORDER BY edge_id, source"
)


def _state(store: Any, branch: str) -> dict[str, list[dict[str, Any]]]:
    return {
        "edges": [dict(row) for row in store.query(_EDGES, branch=branch)],
        "declarers": [
            dict(row) for row in store.query(_DECLARERS, branch=branch)
        ],
    }


def test_a_prepared_ranked_chunk_reuses_its_decision_and_matches_the_direct_path(
    clean_runtime: str,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore

    _rank(clean_runtime, RANKING)
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts(
            [
                _node("sym:a"),
                _node("sym:b"),
                _node("sym:c"),
                _edge("sym:a", "sym:b", "s1"),
                _edge("sym:a", "sym:c", "s1"),
            ],
            source=STRUCTURE,
            strict_endpoints=False,
        )
        store.create_branch("experiment:direct", from_branch="default")
        # The enricher outranks the source: it takes over a -> b and
        # declares a new b -> c; the source keeps a -> c.
        facts = [
            _edge("sym:a", "sym:b", "e1"),
            _edge("sym:b", "sym:c", "e1"),
        ]

        direct = store.apply_facts(
            facts, source=ENRICHER, strict_endpoints=False,
            branch="experiment:direct",
        )
        assert "transaction.edge_declarers.read_current" in direct.timings_ms

        prepared = store.prepare_apply_facts(
            facts, branch="default", source=ENRICHER, strict_endpoints=False,
            key_fingerprint=True,
        )
        assert prepared.ranked_decision is not None
        assert "prepare.edge_declarers.reconcile" in prepared.timings_ms
        result = store.apply_facts(
            facts, source=ENRICHER, strict_endpoints=False, prepared=prepared,
        )
        assert result.timings_ms.get("prepared.reused") == 1.0, result.timings_ms
        assert (
            result.timings_ms.get("transaction.edge_declarers.prepared_reused")
            == 1.0
        ), result.timings_ms
        # Nothing ranked is read again under the lock.
        assert "transaction.edge_declarers.read_current" not in result.timings_ms
        assert "transaction.edge_declarers.read_declarations" not in result.timings_ms

        reused = _state(store, "default")
        assert reused == _state(store, "experiment:direct")
        assert {
            (row["src"], row["dst"], row["source"]) for row in reused["edges"]
        } == {
            ("sym:a", "sym:b", ENRICHER),
            ("sym:a", "sym:c", STRUCTURE),
            ("sym:b", "sym:c", ENRICHER),
        }
        assert len(reused["declarers"]) == 4
    finally:
        store.close()


def test_a_declaration_recorded_after_preparing_makes_the_write_decide_again(
    clean_runtime: str,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore

    _rank(clean_runtime, RANKING)
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts(
            [_node("sym:a"), _node("sym:b")],
            source=STRUCTURE,
            strict_endpoints=False,
        )
        store.apply_facts(
            [_edge("sym:a", "sym:b", "e1")],
            source=ENRICHER,
            strict_endpoints=False,
        )
        # Prepared alone, the enricher's retraction deletes the edge: it is
        # the only declarer.
        withdraw = [_edge("sym:a", "sym:b", "e1", op="R")]
        prepared = store.prepare_apply_facts(
            withdraw, branch="default", source=ENRICHER,
            strict_endpoints=False, key_fingerprint=True,
        )
        assert prepared.ranked_decision is not None
        assert prepared.ranked_decision.outcome.declaration_deletes

        # The lower-ranked source declares the same edge. The stored edge
        # row stays the enricher's, so only the declarations change.
        edges_before = [
            part for part in store._prepared_key_fingerprint(withdraw)
            if part[0] == "okg.graph_edges"
        ]
        store.apply_facts(
            [_edge("sym:a", "sym:b", "s1")],
            source=STRUCTURE,
            strict_endpoints=False,
        )
        edges_after = [
            part for part in store._prepared_key_fingerprint(withdraw)
            if part[0] == "okg.graph_edges"
        ]
        assert edges_after == edges_before
        assert store._prepared_key_fingerprint(withdraw) != prepared.key_fingerprint

        result = store.apply_facts(
            withdraw, source=ENRICHER, strict_endpoints=False,
            prepared=prepared,
        )
        assert (
            result.timings_ms.get("prepared.recomputed.key_rows_changed") == 1.0
        ), result.timings_ms
        assert "transaction.edge_declarers.prepared_reused" not in result.timings_ms
        # The edge passes to the source instead of going.
        state = _state(store, "default")
        assert [
            (row["src"], row["dst"], row["source"], row["attrs"]["via"])
            for row in state["edges"]
        ] == [("sym:a", "sym:b", STRUCTURE, "s1")]
        assert [row["source"] for row in state["declarers"]] == [STRUCTURE]
    finally:
        store.close()
