"""M6 — shadow-diff harness tests."""
from __future__ import annotations

import json

import psycopg
import pytest

from okg.substrate.cutover import (
    DiffBudget,
    EdgeSnapshot,
    GraphSnapshot,
    NodeSnapshot,
    capture_substrate_snapshot,
    shadow_diff,
)
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


# ---------- pure shadow_diff (no DB) ----------

def test_diff_empty_sides() -> None:
    a = GraphSnapshot(origin="a", nodes=frozenset(), edges=frozenset())
    b = GraphSnapshot(origin="b", nodes=frozenset(), edges=frozenset())
    r = shadow_diff(a, b)
    assert r.is_clean
    assert r.a_node_count == 0
    assert r.b_node_count == 0


def test_diff_reports_symmetric_differences() -> None:
    a = GraphSnapshot(
        origin="a",
        nodes=frozenset({
            NodeSnapshot("n:1", "page", "h1"),
            NodeSnapshot("n:2", "page", "h2"),
        }),
        edges=frozenset({
            EdgeSnapshot("n:1", "refs", "n:2"),
        }),
    )
    b = GraphSnapshot(
        origin="b",
        nodes=frozenset({
            NodeSnapshot("n:2", "page", "h2"),
            NodeSnapshot("n:3", "page", "h3"),
        }),
        edges=frozenset({
            EdgeSnapshot("n:2", "refs", "n:3"),
        }),
    )
    r = shadow_diff(a, b)
    assert r.is_clean is False
    assert r.nodes_only_in_a == ["n:1"]
    assert r.nodes_only_in_b == ["n:3"]
    assert r.edges_only_in_a == [("n:1", "refs", "n:2")]
    assert r.edges_only_in_b == [("n:2", "refs", "n:3")]


def test_diff_detects_subtype_mismatch() -> None:
    a = GraphSnapshot(
        origin="a",
        nodes=frozenset({NodeSnapshot("n:1", "page", None)}),
        edges=frozenset(),
    )
    b = GraphSnapshot(
        origin="b",
        nodes=frozenset({NodeSnapshot("n:1", "concept", None)}),
        edges=frozenset(),
    )
    r = shadow_diff(a, b)
    assert r.subtype_mismatches == [{
        "node_id": "n:1", "a_subtype": "page", "b_subtype": "concept",
    }]
    assert r.is_clean is False


def test_diff_detects_text_hash_mismatch() -> None:
    a = GraphSnapshot(
        origin="a",
        nodes=frozenset({NodeSnapshot("n:1", "page", "abc")}),
        edges=frozenset(),
    )
    b = GraphSnapshot(
        origin="b",
        nodes=frozenset({NodeSnapshot("n:1", "page", "xyz")}),
        edges=frozenset(),
    )
    r = shadow_diff(a, b)
    assert len(r.text_hash_mismatches) == 1
    assert r.is_clean is False


def test_diff_skips_text_hash_when_null_on_either_side() -> None:
    """A text_hash of None on EITHER side means "no text", not
    "missing data" — the diff treats it as same."""
    a = GraphSnapshot(
        origin="a",
        nodes=frozenset({NodeSnapshot("n:1", "page", "abc")}),
        edges=frozenset(),
    )
    b = GraphSnapshot(
        origin="b",
        nodes=frozenset({NodeSnapshot("n:1", "page", None)}),
        edges=frozenset(),
    )
    r = shadow_diff(a, b)
    assert r.text_hash_mismatches == []


def test_diff_budget_silences_expected_differences() -> None:
    """An author-declared legitimate diff doesn't flag the
    result as dirty."""
    a = GraphSnapshot(
        origin="a",
        nodes=frozenset({
            NodeSnapshot("n:expected_only_in_a", "page", None),
            NodeSnapshot("n:shared", "page", None),
        }),
        edges=frozenset(),
    )
    b = GraphSnapshot(
        origin="b",
        nodes=frozenset({NodeSnapshot("n:shared", "page", None)}),
        edges=frozenset(),
    )
    budget = DiffBudget(
        allow_nodes_only_in_a=frozenset({"n:expected_only_in_a"}),
    )
    r = shadow_diff(a, b, budget=budget)
    assert r.is_clean
    # Raw counts still reflect reality.
    assert r.raw_counts["raw_nodes_only_in_a"] == 1


def test_diff_budget_subtype_remap() -> None:
    """Legacy says 'Issue', substrate says 'Ticket' — remap
    before compare."""
    a = GraphSnapshot(
        origin="legacy",
        nodes=frozenset({NodeSnapshot("n:1", "Issue", None)}),
        edges=frozenset(),
    )
    b = GraphSnapshot(
        origin="substrate",
        nodes=frozenset({NodeSnapshot("n:1", "Ticket", None)}),
        edges=frozenset(),
    )
    budget = DiffBudget(expected_subtype_remap={"Issue": "Ticket"})
    r = shadow_diff(a, b, budget=budget)
    assert r.subtype_mismatches == []
    assert r.is_clean


def test_diff_sample_cap() -> None:
    """Samples are bounded."""
    a_nodes = frozenset(
        NodeSnapshot(f"n:a{i}", "page", None) for i in range(500)
    )
    a = GraphSnapshot(origin="a", nodes=a_nodes, edges=frozenset())
    b = GraphSnapshot(origin="b", nodes=frozenset(), edges=frozenset())
    r = shadow_diff(a, b, sample_cap=50)
    assert len(r.nodes_only_in_a) == 50
    assert r.raw_counts["raw_nodes_only_in_a"] == 500


# ---------- DB-backed capture ----------

def _seed_cv(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        return cur.fetchone()[0]


def _insert_node_facts(
    conn: psycopg.Connection,
    nodes: list[tuple[str, str, str]],
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts([
        NodeFact(nid, subtype, {"text": text}, {"id": nid}, {"v": 1})
        for nid, subtype, text in nodes
    ])


def test_capture_substrate_snapshot_latest(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _insert_node_facts(conn, [
            ("n:a", "page", "alpha"),
            ("n:b", "page", "beta"),
        ])
    r = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    snap = capture_substrate_snapshot(
        clean_runtime, deployment_name="default",
    )
    assert snap.generation_id == r["generation_id"]
    assert snap.node_count == 2
    assert snap.node_ids == frozenset({"n:a", "n:b"})
    assert "substrate:default:g" in snap.origin


def test_capture_substrate_snapshot_explicit_generation(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _insert_node_facts(conn, [("n:a", "page", "a")])
    r1 = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _insert_node_facts(conn, [("n:b", "page", "b")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    snap = capture_substrate_snapshot(
        clean_runtime, deployment_name="default",
        generation_id=r1["generation_id"],
    )
    assert snap.generation_id == r1["generation_id"]
    assert snap.node_count == 1
    assert snap.node_ids == frozenset({"n:a"})


def test_substrate_self_diff_is_clean(clean_runtime: str) -> None:
    """Capturing the same state twice gives a clean diff — the
    harness's self-consistency check. If this fails, the
    capture itself is non-deterministic."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _insert_node_facts(conn, [
            ("n:a", "page", "alpha"),
            ("n:b", "concept", "beta"),
        ])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    a = capture_substrate_snapshot(clean_runtime, deployment_name="default")
    b = capture_substrate_snapshot(clean_runtime, deployment_name="default")
    r = shadow_diff(a, b)
    assert r.is_clean
    assert r.a_node_count == r.b_node_count == 2


def test_diff_result_serializes_to_json(clean_runtime: str) -> None:
    """as_dict() produces a JSON-serializable payload — used by
    the cutover telemetry emitter."""
    a = GraphSnapshot(
        origin="a",
        nodes=frozenset({NodeSnapshot("n:1", "page", "h")}),
        edges=frozenset({EdgeSnapshot("n:1", "r", "n:2")}),
    )
    b = GraphSnapshot(origin="b", nodes=frozenset(), edges=frozenset())
    r = shadow_diff(a, b)
    blob = json.dumps(r.as_dict())  # must not raise
    round_trip = json.loads(blob)
    assert round_trip["a_origin"] == "a"
    assert round_trip["nodes_only_in_a"] == ["n:1"]
