"""M4.1 — generation-pinned MCP tool tests."""
from __future__ import annotations

import uuid

import psycopg
import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import (
    Session,
    aggregate_edge,
    aggregate_node,
    expand_neighbors,
    expand_path,
    get_ontology,
    inspect_graph,
    inspect_node,
    open_session,
    parse_path_hops,
)
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import SubtypeSpec, seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def traverse_path(conn, session, start_node, *, path, **kwargs):
    """Test bridge: the typed-path traversal now routes through the IR-backed
    `expand_path`. Parsing the path-string grammar here keeps the existing
    path=[...] call sites (and the conn=None arg-validation tests, which raise in
    parse_path_hops before any DB access) working unchanged."""
    return expand_path(
        conn, session, start_node, hops=parse_path_hops(path), **kwargs
    )


def _u() -> str:
    return uuid.uuid4().hex[:12]


def _seed_catalog_version(
    conn: psycopg.Connection,
    *,
    subtypes: list[tuple[str, dict]] = (),
    narrowings: list[tuple[str, str, str]] = (),
    aliases: list[tuple[str, str, str | None]] = (),
) -> int:
    if not subtypes and not narrowings:
        subtypes = [("page", {}), ("concept", {})]
        narrowings = [
            ("page", "mentions", "page"),
            ("page", "mentions", "concept"),
            ("page", "refs", "page"),
            ("page", "refs", "concept"),
            ("page", "references", "concept"),
            ("page", "cites", "page"),
        ]
    return seed_catalog(
        conn,
        subtypes=[
            SubtypeSpec(name, metadata=metadata)
            for name, metadata in subtypes
        ],
        narrowings=narrowings,
        aliases=aliases,
    )


def _insert_node_facts(
    conn: psycopg.Connection,
    nodes: list[tuple[str, str, str]],  # (node_id, subtype, text)
    *,
    source: str = "fixture",
) -> None:
    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts([
        NodeFact(
            node_id=nid,
            subtype=subtype,
            attrs={"text": text},
            source_record_id={"id": nid, "source": source},
            source_revision={"v": 1},
        )
        for nid, subtype, text in nodes
    ])


def _insert_edge_facts(
    conn: psycopg.Connection,
    edges: list[tuple[str, str, str]],  # (src, dst, edge_type)
    *,
    source: str = "fixture",
) -> None:
    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts([
        EdgeFact(
            src=src,
            dst=dst,
            edge_type=etype,
            attrs={},
            source_record_id={"src": src, "dst": dst, "source": source},
            source_revision={"v": 1},
        )
        for src, dst, etype in edges
    ])


# ---------- session + pin resolution ----------

def test_session_fingerprint_mismatch_raises(clean_runtime: str) -> None:
    """M4.5 correctness regression (Dijkstra P0 #9).

    A Session opened against one DB must not work with a
    connection to a different DB. Simulated here by opening the
    session then stamping a different fingerprint on it.
    """
    from dataclasses import replace
    from okg.substrate.errors import SubstrateError
    from okg.substrate.mcp.session import assert_session_matches

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _insert_node_facts(conn, [("n:a", "page", "x")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        # Session opened on this DB: matches.
        assert_session_matches(conn, session)
        # Forge a different DB fingerprint on the session.
        forged = replace(session, db_fingerprint="other_db:99999")
        with pytest.raises(SubstrateError, match="different database"):
            assert_session_matches(conn, forged)
        # Every tool entry enforces the same guard.
        with pytest.raises(SubstrateError, match="different database"):
            inspect_graph(conn, forged)


def test_open_session_pins_latest_published(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _insert_node_facts(conn, [("n:1", "page", "hello")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
    assert s.deployment_name == "default"
    assert s.generation_id.startswith("okg:")
    assert s.catalog_version_id > 0


def test_open_session_accepts_explicit_generation(
    clean_runtime: str,
) -> None:
    """A caller may pin to an older published generation."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _insert_node_facts(conn, [("n:1", "page", "t1")])
    r1 = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _insert_node_facts(conn, [("n:2", "page", "t2")])
    r2 = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(
            conn, deployment_name="default",
            requested_generation=r1["generation_id"],
        )
    assert s.generation_id == r1["generation_id"]
    assert s.generation_id != r2["generation_id"]


def test_open_session_rejects_missing_generation(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(SubstrateError, match="no published generation"):
            open_session(conn, deployment_name="default")


def test_open_session_rejects_unpublished_generation(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _insert_node_facts(conn, [("n:1", "page", "t")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(SubstrateError, match="is not a published OKG generation"):
            open_session(
                conn, deployment_name="default",
                requested_generation=99999,
            )


# ---------- tool basics ----------

def test_graph_stats_counts_at_pin(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _insert_node_facts(conn, [
            (f"n:{i}", "page", f"t{i}") for i in range(5)
        ])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        stats = inspect_graph(conn, s)
    assert stats["node_count"] == 5
    assert stats["edge_count"] == 0
    assert stats["subtype_count"] == 1


def test_get_node_returns_at_pin(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _insert_node_facts(conn, [
            ("n:a", "page", "alpha text"),
            ("n:b", "page", "beta text"),
        ])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        node = inspect_node(conn, s, "n:a")
    assert node is not None
    assert node["node_id"] == "n:a"
    assert node["subtype"] == "page"
    assert node["attrs"]["text"] == "alpha text"


def test_get_node_missing_returns_none(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _insert_node_facts(conn, [("n:a", "page", "x")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        assert inspect_node(conn, s, "n:missing") is None


def test_list_neighbors_at_pin(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _insert_node_facts(conn, [
            ("n:src", "page", "src"),
            ("n:d1", "page", "d1"),
            ("n:d2", "page", "d2"),
        ])
        _insert_edge_facts(conn, [
            ("n:src", "n:d1", "mentions"),
            ("n:src", "n:d2", "mentions"),
        ])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        neighbors = expand_neighbors(conn, s, "n:src")
    dsts = sorted(n["dst"] for n in neighbors["neighbors"])
    assert dsts == ["n:d1", "n:d2"]
    assert all(n["edge_type"] == "mentions" for n in neighbors["neighbors"])
    assert neighbors["truncated"] is False


def test_aggregate_edge_routes_filters_subtype_triple(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(
            conn,
            subtypes=[
                ("page", {}),
                ("concept", {}),
            ],
            narrowings=[
                ("page", "mentions", "page"),
                ("page", "references", "concept"),
            ],
        )
        _insert_node_facts(conn, [
            ("n:src", "page", "src"),
            ("n:d1", "page", "d1"),
            ("n:d2", "page", "d2"),
            ("n:other", "concept", "other"),
        ])
        _insert_edge_facts(conn, [
            ("n:src", "n:d1", "mentions"),
            ("n:src", "n:d2", "mentions"),
            ("n:src", "n:other", "references"),
        ])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        out = aggregate_edge(
            conn,
            s,
            src_subtype="page",
            edge_type="mentions",
            dst_subtype="page",
        )
    assert out["filters"] == {
        "src_subtype": "page",
        "edge_type": "mentions",
        "dst_subtype": "page",
    }
    assert out["routes"] == [{
        "src_subtype": "page",
        "edge_type": "mentions",
        "dst_subtype": "page",
        "count": 2,
    }]
    assert out["total_count"] == 1
    assert out["truncated"] is False


def test_get_ontology_at_pin(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn, subtypes=[
            ("page", {"description": "A web page"}),
            ("concept", {"description": "A canonical concept"}),
        ], narrowings=[
            ("page", "references", "concept"),
        ], aliases=[
            ("foo", "n:x", None),
            ("bar", "n:y", "concept"),
        ])
        _insert_node_facts(conn, [("n:1", "page", "t")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        onto = get_ontology(conn, s)
    assert len(onto["subtypes"]) == 2
    assert any(st["name"] == "page" for st in onto["subtypes"])
    assert onto["narrowings"] == [
        {"parent": "page", "edge": "references", "child": "concept"}
    ]
    assert onto["alias_count"] == 2


# ---------- the headline M4 invariant: pin stability ----------

def test_pin_stable_across_concurrent_publish(clean_runtime: str) -> None:
    """Tasks.md 4.1 acceptance: a session pinned to generation N
    returns stable counts / neighbors / aggregates while generation
    N+1 publishes concurrently."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _insert_node_facts(conn, [
            (f"n:{i}", "page", f"t{i}") for i in range(5)
        ])
    r1 = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    # Open a session pinned to gen N.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session_n = open_session(conn, deployment_name="default")
    assert session_n.generation_id == r1["generation_id"]

    # Capture a snapshot of observable state AT the pin.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        stats_before = inspect_graph(conn, session_n)

    # Publish gen N+1 with a fundamentally different node set
    # (3 new nodes + 1 updated text) — would change all three
    # readouts above if the pin were ignored.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _insert_node_facts(conn, [
            ("n:new1", "concept", "new1 text"),
            ("n:new2", "concept", "new2 text"),
            ("n:new3", "concept", "new3 text"),
            ("n:0", "page", "t0 changed"),  # same node, new text
        ])
    r2 = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert r2["generation_id"] != r1["generation_id"]

    # Re-read via the OLD session → same numbers as before.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        stats_after = inspect_graph(conn, session_n)

    # `embed_lag` is deliberately NOT pinned — it's an
    # operational metric about the latest pipeline state, not a
    # pin-consistent view. Strip it before comparing.
    assert {k: v for k, v in stats_after.items() if k != "embed_lag"} == \
           {k: v for k, v in stats_before.items() if k != "embed_lag"}, (
        f"pinned graph_stats fields changed: before={stats_before} "
        f"after={stats_after}"
    )

    # Sanity: a fresh session on the LATEST generation sees
    # the new data, confirming N+1 really did publish.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session_n1 = open_session(conn, deployment_name="default")
        assert session_n1.generation_id == r2["generation_id"]
        stats_latest = inspect_graph(conn, session_n1)
    assert stats_latest["node_count"] == 5 + 3


# ---------- traverse_path ----------

def test_traverse_path_one_hop(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _insert_node_facts(conn, [
            ("n:a", "page", "a"),
            ("n:b", "page", "b"),
            ("n:c", "page", "c"),
        ])
        _insert_edge_facts(conn, [
            ("n:a", "n:b", "refs"),
            ("n:a", "n:c", "refs"),
        ])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        out = traverse_path(conn, s, "n:a", path=["refs:out"])
    ids = sorted(r["node_id"] for r in out["results"])
    assert ids == ["n:b", "n:c"]
    assert out["truncated"] is False


def test_traverse_path_two_hops(clean_runtime: str) -> None:
    """a --refs--> b --mentions--> c : reach c via 2-hop path."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _insert_node_facts(conn, [
            ("n:a", "page", "a"),
            ("n:b", "page", "b"),
            ("n:c", "concept", "c"),
        ])
        _insert_edge_facts(conn, [
            ("n:a", "n:b", "refs"),
            ("n:b", "n:c", "mentions"),
        ])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        out = traverse_path(
            conn, s, "n:a",
            path=["refs:out", "mentions:out"],
        )
    ids = sorted(r["node_id"] for r in out["results"])
    assert ids == ["n:c"]
    assert out["results"][0]["subtype"] == "concept"


def test_traverse_path_in_direction(clean_runtime: str) -> None:
    """Reverse direction: find who REFS a given node."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _insert_node_facts(conn, [
            ("n:a", "page", "a"),
            ("n:b", "page", "b"),
            ("n:t", "page", "target"),
        ])
        _insert_edge_facts(conn, [
            ("n:a", "n:t", "refs"),
            ("n:b", "n:t", "refs"),
        ])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        out = traverse_path(conn, s, "n:t", path=["refs:in"])
    ids = sorted(r["node_id"] for r in out["results"])
    assert ids == ["n:a", "n:b"]


def test_traverse_path_target_subtype_filter(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _insert_node_facts(conn, [
            ("n:a", "page", "a"),
            ("n:b", "page", "b"),
            ("n:c", "concept", "c"),
        ])
        _insert_edge_facts(conn, [
            ("n:a", "n:b", "refs"),
            ("n:a", "n:c", "refs"),
        ])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        out = traverse_path(
            conn, s, "n:a", path=["refs:out"],
            target_subtype="concept",
        )
    ids = [r["node_id"] for r in out["results"]]
    assert ids == ["n:c"]


def test_traverse_path_missing_start_node_error(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _insert_node_facts(conn, [("n:a", "page", "a")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        out = traverse_path(conn, s, "n:missing", path=["refs:out"])
    assert out["results"] == []
    assert "not visible" in out.get("error", "")


def test_traverse_path_rejects_too_deep() -> None:
    s = Session(
        deployment_name="x", generation_id="generation:test", catalog_version_id=1,
        db_fingerprint="test:0",
    )
    # Don't even need a live DB — rejected at arg validation.
    with pytest.raises(ValueError, match="exceeds the maximum of 3 hops"):
        traverse_path(
            None, s, "n:a",  # type: ignore[arg-type]
            path=["a:out", "b:out", "c:out", "d:out"],
        )


def test_traverse_path_rejects_malformed_hop() -> None:
    s = Session(
        deployment_name="x", generation_id="generation:test", catalog_version_id=1,
        db_fingerprint="test:0",
    )
    with pytest.raises(ValueError, match="must use 'edge_type:out' or 'edge_type:in'"):
        traverse_path(None, s, "n:a", path=["refs"])  # no :direction
    with pytest.raises(ValueError, match="direction must be 'out' or 'in'"):
        traverse_path(None, s, "n:a", path=["refs:sideways"])


def test_traverse_path_honors_pin(clean_runtime: str) -> None:
    """Traverse output stays stable when a new generation adds
    more edges."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _insert_node_facts(conn, [
            ("n:a", "page", "a"),
            ("n:b", "page", "b"),
        ])
        _insert_edge_facts(conn, [("n:a", "n:b", "refs")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session_n = open_session(conn, deployment_name="default")
        out_before = traverse_path(conn, session_n, "n:a", path=["refs:out"])

    # Add another target + another edge in a new publish cycle.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _insert_node_facts(conn, [("n:c", "page", "c")])
        _insert_edge_facts(conn, [("n:a", "n:c", "refs")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        out_after = traverse_path(
            conn, session_n, "n:a", path=["refs:out"],
        )
    assert out_after == out_before, (
        f"traverse_path perturbed under pin: before={out_before} "
        f"after={out_after}"
    )


def test_aggregate_neighbors(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _insert_node_facts(conn, [
            ("n:src", "page", "src"),
            ("n:d1", "concept", "d1"),
            ("n:d2", "concept", "d2"),
            ("n:d3", "page", "d3"),
        ])
        _insert_edge_facts(conn, [
            ("n:src", "n:d1", "references"),
            ("n:src", "n:d2", "references"),
            ("n:src", "n:d3", "cites"),
        ])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        agg = aggregate_node(conn, s, "n:src")
    buckets = {
        (c["edge_type"], c["dst_subtype"]): c["count"] for c in agg["counts"]
    }
    assert buckets == {
        ("references", "concept"): 2,
        ("cites", "page"): 1,
    }
