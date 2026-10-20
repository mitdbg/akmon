"""Delta-aware Chronos publish evidence (deltaflow-publish-projection /
req.projection-deltas, task.implement-projection).

Proves the real publish path produces and *records* a faithful per-generation
"change receipt": graph upserts and retractions, a no-op publish that changes
nothing, retractions that actually expire the live fact, and unaffected scopes
that are not rewritten. The receipt is stamped into the generation's checkpoint
metadata (what `okg generation describe` reads), so the evidence survives the
cycle rather than being recomputed.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

_DELTA_KEYS = (
    "nodes_added",
    "nodes_retracted",
    "nodes_updated",
    "edges_added",
    "edges_retracted",
    "edges_updated",
)


def _node(node_id: str, *, v: int = 1, op: str = "I") -> NodeFact:
    return NodeFact(
        node_id=node_id,
        subtype="publication",
        attrs={},
        source_record_id={"id": node_id},
        source_revision={"v": v},
        op=op,
    )


def _apply(dsn: str, facts: list[NodeFact]) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            facts, source="test.delta_projection"
        )


def _publish(dsn: str) -> dict:
    return Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()


def _receipt(dsn: str, generation_id) -> dict | None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        cp = ChronosGraphStore.from_connection(conn).get_generation(generation_id)
        return dict(cp.metadata or {}).get("projection_deltas")


def _live_node_ids(dsn: str, node_ids: list[str]) -> set[str]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(conn).query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id = ANY(%(ids)s)",
            {"ids": node_ids},
        )
    return {r["node_id"] for r in rows}


def _assert_receipt_matches_cycle(receipt: dict | None, out: dict) -> None:
    assert receipt is not None, "publish recorded no projection_deltas receipt"
    for key in _DELTA_KEYS:
        assert receipt[key] == out[key], (
            f"receipt {key}={receipt[key]} != cycle {key}={out[key]}"
        )
    assert receipt["noop"] == out["noop"]


def test_publish_records_upsert_receipt(clean_runtime: str) -> None:
    """An insert publish records upserts, and the checkpoint receipt is a
    faithful copy of the cycle's computed deltas."""
    _apply(clean_runtime, [_node("publication:a"), _node("publication:b")])
    out = _publish(clean_runtime)

    assert out["status"] == "published"
    assert out["nodes_added"] >= 2  # both seeded nodes are upserts
    assert out["nodes_retracted"] == 0
    _assert_receipt_matches_cycle(_receipt(clean_runtime, out["generation_id"]), out)


def test_retraction_does_not_leave_live_provenance(clean_runtime: str) -> None:
    """A retraction expires exactly the retracted node and rewrites nothing
    else: req.projection-deltas scn.retraction + RetractionDoesNotLeaveLive."""
    _apply(clean_runtime, [_node("publication:a"), _node("publication:b")])
    g1 = _publish(clean_runtime)
    assert g1["status"] == "published"
    assert _live_node_ids(clean_runtime, ["publication:a", "publication:b"]) == {
        "publication:a",
        "publication:b",
    }

    _apply(clean_runtime, [_node("publication:a", op="R")])
    g2 = _publish(clean_runtime)

    assert g2["status"] == "published"
    assert g2["nodes_retracted"] >= 1
    assert g2["nodes_added"] == 0  # the unaffected node b is NOT re-added
    assert g2["node_count"] == g1["node_count"] - 1  # net one fewer live node
    # The retracted fact is actually gone; the unaffected one survives.
    assert _live_node_ids(clean_runtime, ["publication:a", "publication:b"]) == {
        "publication:b"
    }
    _assert_receipt_matches_cycle(_receipt(clean_runtime, g2["generation_id"]), g2)


def test_no_change_publish_is_a_noop(clean_runtime: str) -> None:
    """A publish with nothing pending is a no-op: zero cycle deltas, and it
    reuses the prior generation rather than publishing a fresh checkpoint."""
    _apply(clean_runtime, [_node("publication:a")])
    g1 = _publish(clean_runtime)

    out = _publish(clean_runtime)  # nothing changed since the last publish
    assert out["noop"] is True
    for key in _DELTA_KEYS:
        assert out[key] == 0, f"no-op publish changed {key}={out[key]}"
    # A no-op publishes nothing new — it reuses the prior generation, so its
    # receipt is that generation's, not a fresh all-zero stamp.
    assert out["generation_id"] == g1["generation_id"]
