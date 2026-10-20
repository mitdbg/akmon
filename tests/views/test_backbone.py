"""Postgres-backed tests for okg.views.backbone — Stage 2.

These tests need a real substrate (the substrate's
``clean_runtime`` fixture migrates ``okg.*`` and TRUNCATEs runtime
tables). They exercise:

  * ``resolve_published_generation`` — finds latest, raises on missing.
  * ``corpus_from_okg`` — generation pin, selectors, anchor builders,
    per-generation dedup, and the "no-deployment_name argument" rule.
  * ``edges_at_generation`` — edge_type filter + pin.
  * Memoization — same key → cache hit; different generation → miss.

Post-Chronos-cutover: ``corpus_from_okg`` / ``edges_at_generation``
read the Chronos graph (``okg.graph_nodes`` / ``okg.graph_edges``
via a generation checkout), NOT the legacy ``okg.nodes_live`` /
``okg.edges_live`` tables. Test data is therefore seeded by applying
``NodeFact`` / ``EdgeFact`` to a :class:`ChronosGraphStore` branch and
checkpointing each pin with the integer ``generation_id`` the reader
pins to. ``okg.graph_generations`` rows are still inserted (via
``_insert_generation``) so ``resolve_published_generation`` resolves a
pin and the checkpoint id (``generation:<gen>``) lines up with the
checkout the adapter performs.
"""
from __future__ import annotations

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.views.backbone import (
    BackboneError,
    NoPublishedGenerationError,
    NodeRow,
    by_type,
    by_type_and_attrs,
    clear_corpus_cache,
    corpus_from_okg,
    corpus_from_okg_cached,
    edges_at_generation,
    resolve_published_generation,
)
from okg.views.types import Anchor
import psycopg
import pytest
from tests.pytest.catalog_fixture_builders import seed_narrowings

pytestmark = pytest.mark.postgres


# ---------------------------------------------------------------------------
# Fixtures: seed test data into the Chronos graph
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _ensure_chronos_schema(clean_runtime):
    """Ensure the Chronos schema + default branch exist for the seed helpers.

    The views ``clean_runtime`` already RESETS the Chronos graph per-test (finding
    G3, via reset_chronos_graph), so isolation is handled. But on a fresh DB that
    reset is a no-op (no ``public._chronos_*`` tables yet) and the seed helpers below
    use ``ensure_schema=False`` -- so create the schema/default branch here so the
    first test in the file has somewhere to write.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as c:
        ChronosGraphStore.from_connection(c, ensure_schema=True)
    yield


@pytest.fixture
def conn(clean_runtime):
    """Open a connection on the migrated, truncated test DB.

    ``autocommit=True`` matches the substrate's read-path style
    (cutover/snapshot, mcp/server). Each test owns its connection
    so test-level cleanup is bounded.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as c:
        yield c


@pytest.fixture(autouse=True)
def _clear_views_cache():
    """Each test starts with an empty corpus cache so memoization
    doesn't leak across test cases.
    """
    clear_corpus_cache()
    yield
    clear_corpus_cache()


def _insert_generation(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    status: str = "published",
    catalog_version_id: int = 1,  # the seed row from migrate
) -> int:
    """Insert a graph_generations row, return the generation_id.

    Uses a minimal INSERT that satisfies the schema's NOT NULL
    constraints (deployment_name, status, catalog_version_id,
    correlation_id). We don't go through the publisher because
    we want to control exactly what's visible at each pin.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.graph_generations
              (deployment_name, status, catalog_version_id,
               correlation_id, published_at)
            VALUES (%s, %s, %s, %s, now())
            RETURNING generation_id
            """,
            (
                deployment_name,
                status,
                catalog_version_id,
                f"test-{deployment_name}",
            ),
        )
        return int(cur.fetchone()[0])


def _ensure_narrowing(
    conn: psycopg.Connection,
    parent_subtype: str,
    edge_type: str,
    child_subtype: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE((
              SELECT max(catalog_version_id)
                FROM okg.catalog_versions
               WHERE status = 'applied'
            ), 1)
            """
        )
        catalog_version_id = int(cur.fetchone()[0])
    seed_narrowings(
        conn,
        catalog_version_id=catalog_version_id,
        narrowings=[(parent_subtype, edge_type, child_subtype)],
    )


def _apply_nodes(
    conn: psycopg.Connection,
    nodes: list[tuple[str, str, dict]],
) -> None:
    """Apply node upserts to the Chronos default branch.

    Each tuple is ``(node_id, subtype, attrs)``. The backbone
    adapter reads ``okg.graph_nodes`` via a generation checkout, so
    test data lives in the Chronos graph rather than ``nodes_live``.
    Same ids / subtypes / attrs the legacy SQL seed used.
    """
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    store.apply_facts(
        [
            NodeFact(
                node_id=node_id,
                subtype=subtype,
                attrs=dict(attrs),
                source_record_id={"id": node_id},
                source_revision={"v": 1},
            )
            for node_id, subtype, attrs in nodes
        ],
        source="default",
        strict_endpoints=True,
    )


def _retract_nodes(
    conn: psycopg.Connection,
    nodes: list[tuple[str, str]],
) -> None:
    """Retract nodes (``op='R'``) on the Chronos default branch.

    Each tuple is ``(node_id, subtype)``. Used by the pin-window
    tests to close a node's visibility before the next checkpoint.
    """
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    store.apply_facts(
        [
            NodeFact(
                node_id=node_id,
                subtype=subtype,
                attrs={},
                source_record_id={"id": node_id},
                source_revision={"v": 1},
                op="R",
            )
            for node_id, subtype in nodes
        ],
        source="default",
        strict_endpoints=True,
    )


def _apply_edges(
    conn: psycopg.Connection,
    edges: list[tuple[str, str, str, dict]],
) -> None:
    """Apply edge upserts to the Chronos default branch.

    Each tuple is ``(src, dst, edge_type, attrs)``. The Chronos
    store derives ``edge_id`` from ``(src, edge_type, dst)``; the
    backbone adapter never exposes the legacy serial ``edge_id``,
    so callers assert on ``src`` / ``dst`` / ``attrs`` only.
    """
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    store.apply_facts(
        [
            EdgeFact(
                src=src,
                dst=dst,
                edge_type=edge_type,
                attrs=dict(attrs),
                source_record_id={"id": f"{src}->{dst}:{edge_type}"},
                source_revision={"v": 1},
            )
            for src, dst, edge_type, attrs in edges
        ],
        source="default",
        strict_endpoints=True,
    )


def _checkpoint(conn: psycopg.Connection, generation_id: int) -> None:
    """Checkpoint the current Chronos branch head as ``generation_id``.

    The backbone adapter pins reads with ``checkout_generation(gen)``,
    which resolves the checkpoint ``generation:<gen>``; this helper
    creates that checkpoint capturing the branch state at call time.
    """
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    store.checkpoint_generation(generation_id, deployment_name="test")


# ---------------------------------------------------------------------------
# resolve_published_generation
# ---------------------------------------------------------------------------


class TestResolvePublishedGeneration:
    def test_returns_latest_published(self, conn):
        g1 = _insert_generation(conn, deployment_name="test")
        g2 = _insert_generation(conn, deployment_name="test")
        latest = resolve_published_generation(conn, "test")
        assert latest == max(g1, g2)

    def test_ignores_non_published(self, conn):
        # 'pending' generation must NOT be returned as published.
        g1 = _insert_generation(conn, deployment_name="test")
        _insert_generation(conn, deployment_name="test", status="pending")
        latest = resolve_published_generation(conn, "test")
        assert latest == g1

    def test_isolates_by_deployment(self, conn):
        # Other deployments' generations don't leak.
        _insert_generation(conn, deployment_name="other")
        g_test = _insert_generation(conn, deployment_name="test")
        _insert_generation(conn, deployment_name="other")
        assert resolve_published_generation(conn, "test") == g_test

    def test_missing_deployment_raises(self, conn):
        with pytest.raises(NoPublishedGenerationError) as excinfo:
            resolve_published_generation(conn, "nonexistent")
        assert "nonexistent" in str(excinfo.value)


# ---------------------------------------------------------------------------
# corpus_from_okg
# ---------------------------------------------------------------------------


def _catalog_anchor_builder(row: NodeRow) -> list[Anchor]:
    """Build one catalog anchor for every publication node.

    Mirrors the views_demo's anchor shape but reads from
    NodeRow attrs (substrate-native) instead of a snapshot dict.
    """
    if row.subtype != "publication":
        return []
    attrs = row.attrs
    content = (
        f"pub_number: {attrs.get('pub_number', '')}\n"
        f"title: {attrs.get('title', '')}\n"
        f"status: {attrs.get('status', '')}"
    )
    return [Anchor.from_content(
        id=f"{row.node_id}:catalog",
        content=content,
    )]


class TestCorpusFromOkg:
    def test_basic_projection(self, conn):
        gen = _insert_generation(conn, deployment_name="test")
        _apply_nodes(
            conn,
            [(
                "publication:afi10-1001",
                "publication",
                {
                    "pub_number": "AFI10-1001",
                    "title": "Air Operations",
                    "status": "current",
                },
            )],
        )
        _checkpoint(conn, gen)
        corpus = corpus_from_okg(
            conn,
            generation_id=gen,
            selectors=[by_type("publication")],
            anchor_builders=[_catalog_anchor_builder],
        )
        assert "publication:afi10-1001:catalog" in corpus
        anchor = corpus["publication:afi10-1001:catalog"]
        assert "AFI10-1001" in anchor.content
        assert "current" in anchor.content

    def test_excludes_nodes_outside_pin_window(self, conn):
        # Two generations: gen1 has node A, gen2 has node A retired
        # plus node B added.
        gen1 = _insert_generation(conn, deployment_name="test")
        gen2 = _insert_generation(conn, deployment_name="test")
        # gen1 checkpoint captures A only.
        _apply_nodes(
            conn,
            [(
                "publication:a",
                "publication",
                {"pub_number": "A", "title": "alpha", "status": "current"},
            )],
        )
        _checkpoint(conn, gen1)
        # gen2: A retired, B added.
        _retract_nodes(conn, [("publication:a", "publication")])
        _apply_nodes(
            conn,
            [(
                "publication:b",
                "publication",
                {"pub_number": "B", "title": "beta", "status": "current"},
            )],
        )
        _checkpoint(conn, gen2)

        # At gen1: only A is visible.
        corpus_at_g1 = corpus_from_okg(
            conn,
            generation_id=gen1,
            selectors=[by_type("publication")],
            anchor_builders=[_catalog_anchor_builder],
        )
        assert "publication:a:catalog" in corpus_at_g1
        assert "publication:b:catalog" not in corpus_at_g1

        # At gen2: only B is visible (A's window closed at gen2).
        corpus_at_g2 = corpus_from_okg(
            conn,
            generation_id=gen2,
            selectors=[by_type("publication")],
            anchor_builders=[_catalog_anchor_builder],
        )
        assert "publication:a:catalog" not in corpus_at_g2
        assert "publication:b:catalog" in corpus_at_g2

    def test_distinct_on_dedups_within_pin_window(self, conn):
        # The same node_id is upserted twice across generations; the
        # checkout must return the newest revision (one row per node).
        gen1 = _insert_generation(conn, deployment_name="test")
        gen2 = _insert_generation(conn, deployment_name="test")
        pin = _insert_generation(conn, deployment_name="test")
        # Older revision at gen1.
        _apply_nodes(
            conn,
            [(
                "publication:a",
                "publication",
                {"pub_number": "A", "title": "old title", "status": "current"},
            )],
        )
        _checkpoint(conn, gen1)
        # Newer revision at gen2 (upsert overwrites by node_id PK).
        _apply_nodes(
            conn,
            [(
                "publication:a",
                "publication",
                {"pub_number": "A", "title": "new title", "status": "current"},
            )],
        )
        _checkpoint(conn, gen2)
        # Pin after both revisions: the checkout returns the latest
        # revision for the node_id ("new title").
        _checkpoint(conn, pin)
        corpus = corpus_from_okg(
            conn,
            generation_id=pin,
            selectors=[by_type("publication")],
            anchor_builders=[_catalog_anchor_builder],
        )
        assert "new title" in corpus["publication:a:catalog"].content
        assert "old title" not in corpus["publication:a:catalog"].content

    def test_multiple_selectors_compose_with_or(self, conn):
        gen = _insert_generation(conn, deployment_name="test")
        _apply_nodes(
            conn,
            [
                (
                    "publication:a", "publication",
                    {"pub_number": "A", "title": "P", "status": "current"},
                ),
                ("organization:af-a3", "organization", {"name": "AF/A3"}),
                ("person:colonel", "person", {"name": "X"}),
            ],
        )
        _checkpoint(conn, gen)

        # Build an anchor for organizations too so we can detect
        # multi-selector composition.
        def org_builder(row: NodeRow) -> list[Anchor]:
            if row.subtype != "organization":
                return []
            return [Anchor.from_content(
                id=f"{row.node_id}:catalog",
                content=f"org: {row.attrs.get('name', '')}",
            )]

        corpus = corpus_from_okg(
            conn,
            generation_id=gen,
            selectors=[by_type("publication"), by_type("organization")],
            anchor_builders=[_catalog_anchor_builder, org_builder],
        )
        # Publication AND organization are both included; person
        # (third subtype) is not.
        assert "publication:a:catalog" in corpus
        assert "organization:af-a3:catalog" in corpus
        assert all("person" not in k for k in corpus)

    def test_attrs_match_filter(self, conn):
        gen = _insert_generation(conn, deployment_name="test")
        _apply_nodes(
            conn,
            [
                (
                    "publication:a", "publication",
                    {"pub_number": "A", "opr": "AF/A3", "status": "current"},
                ),
                (
                    "publication:b", "publication",
                    {"pub_number": "B", "opr": "AF/A4", "status": "current"},
                ),
            ],
        )
        _checkpoint(conn, gen)
        corpus = corpus_from_okg(
            conn,
            generation_id=gen,
            selectors=[by_type_and_attrs(
                "publication",
                attrs_match=(("opr", "AF/A3"),),
            )],
            anchor_builders=[_catalog_anchor_builder],
        )
        assert "publication:a:catalog" in corpus
        assert "publication:b:catalog" not in corpus

    def test_empty_selectors_returns_empty_corpus(self, conn):
        gen = _insert_generation(conn, deployment_name="test")
        _apply_nodes(
            conn,
            [(
                "publication:a", "publication",
                {"pub_number": "A", "title": "P", "status": "current"},
            )],
        )
        _checkpoint(conn, gen)
        # No selectors → return empty (NOT "all nodes in the
        # generation"). Defensive: prevents accidental full-table
        # scans when a planner forgets to declare scope.
        corpus = corpus_from_okg(
            conn,
            generation_id=gen,
            selectors=[],
            anchor_builders=[_catalog_anchor_builder],
        )
        assert corpus == {}

    def test_non_int_generation_id_raises(self, conn):
        with pytest.raises(TypeError, match="generation_id must be an int"):
            corpus_from_okg(
                conn,
                generation_id="42",  # type: ignore[arg-type]
                selectors=[by_type("publication")],
                anchor_builders=[_catalog_anchor_builder],
            )

    def test_non_int_generation_id_rejects_bool(self, conn):
        # Python's bool is a subclass of int — guard against it
        # so True/False can't slip through.
        with pytest.raises(TypeError, match="generation_id must be an int"):
            corpus_from_okg(
                conn,
                generation_id=True,  # type: ignore[arg-type]
                selectors=[by_type("publication")],
                anchor_builders=[_catalog_anchor_builder],
            )

    def test_anchor_id_collision_raises(self, conn):
        gen = _insert_generation(conn, deployment_name="test")
        _apply_nodes(
            conn,
            [(
                "publication:a", "publication",
                {"pub_number": "A", "title": "P", "status": "current"},
            )],
        )
        _checkpoint(conn, gen)

        # Two builders that both claim the same id namespace —
        # this is a programmer error and must surface loudly.
        def colliding_builder(row: NodeRow) -> list[Anchor]:
            if row.subtype != "publication":
                return []
            return [Anchor.from_content(
                id=f"{row.node_id}:catalog",  # same id as default
                content="conflicting",
            )]

        with pytest.raises(BackboneError, match="anchor id collision"):
            corpus_from_okg(
                conn,
                generation_id=gen,
                selectors=[by_type("publication")],
                anchor_builders=[
                    _catalog_anchor_builder,
                    colliding_builder,
                ],
            )


# ---------------------------------------------------------------------------
# edges_at_generation
# ---------------------------------------------------------------------------


class TestEdgesAtGeneration:
    def test_returns_edges_of_requested_type(self, conn):
        gen = _insert_generation(conn, deployment_name="test")
        _ensure_narrowing(conn, "publication", "supersedes", "publication")
        _ensure_narrowing(conn, "publication", "references", "publication")
        _apply_nodes(
            conn,
            [
                (node_id, "publication",
                 {"pub_number": node_id, "status": "current"})
                for node_id in (
                    "publication:a",
                    "publication:b",
                    "publication:c",
                    "publication:d",
                )
            ],
        )
        _apply_edges(
            conn,
            [
                ("publication:a", "publication:b", "supersedes",
                 {"reason": "rewrite"}),
                ("publication:c", "publication:d", "references", {}),
            ],
        )
        _checkpoint(conn, gen)

        sup = edges_at_generation(
            conn, generation_id=gen, edge_type="supersedes",
        )
        assert len(sup) == 1
        assert sup[0]["src"] == "publication:a"
        assert sup[0]["dst"] == "publication:b"
        assert sup[0]["attrs"]["reason"] == "rewrite"

        ref = edges_at_generation(
            conn, generation_id=gen, edge_type="references",
        )
        assert len(ref) == 1

        # No matching edge_type → empty list, not error.
        none = edges_at_generation(
            conn, generation_id=gen, edge_type="cites",
        )
        assert none == []

    def test_excludes_edges_outside_pin_window(self, conn):
        gen1 = _insert_generation(conn, deployment_name="test")
        gen2 = _insert_generation(conn, deployment_name="test")
        _ensure_narrowing(conn, "publication", "supersedes", "publication")
        # gen1 checkpoint captures the endpoint nodes but no edge.
        _apply_nodes(
            conn,
            [
                (node_id, "publication",
                 {"pub_number": node_id, "status": "current"})
                for node_id in ("a", "b")
            ],
        )
        _checkpoint(conn, gen1)
        # gen2: the edge is added.
        _apply_edges(conn, [("a", "b", "supersedes", {})])
        _checkpoint(conn, gen2)
        # At gen1: not yet visible.
        assert edges_at_generation(
            conn, generation_id=gen1, edge_type="supersedes",
        ) == []
        # At gen2: visible.
        assert len(edges_at_generation(
            conn, generation_id=gen2, edge_type="supersedes",
        )) == 1


# ---------------------------------------------------------------------------
# Memoization
# ---------------------------------------------------------------------------


class TestMemoization:
    def test_cache_hit_skips_postgres(self, conn):
        gen = _insert_generation(conn, deployment_name="test")
        _apply_nodes(
            conn,
            [(
                "publication:a", "publication",
                {"pub_number": "A", "title": "P", "status": "current"},
            )],
        )
        _checkpoint(conn, gen)
        selectors = [by_type("publication")]
        builders = [_catalog_anchor_builder]

        # First call: actually queries Postgres.
        first = corpus_from_okg_cached(
            conn,
            generation_id=gen,
            selectors=selectors,
            anchor_builders=builders,
        )
        # Second call: must be the SAME Python dict (cache hit
        # returns the cached object, not a fresh re-projection).
        second = corpus_from_okg_cached(
            conn,
            generation_id=gen,
            selectors=selectors,
            anchor_builders=builders,
        )
        assert second is first

    def test_different_generation_misses_cache(self, conn):
        gen1 = _insert_generation(conn, deployment_name="test")
        gen2 = _insert_generation(conn, deployment_name="test")
        _apply_nodes(
            conn,
            [(
                "publication:a", "publication",
                {"pub_number": "A", "title": "P", "status": "current"},
            )],
        )
        _checkpoint(conn, gen1)
        # gen2 captures the same node set (no new facts); the cache
        # keys on generation_id so the two pins are distinct entries.
        _checkpoint(conn, gen2)
        selectors = [by_type("publication")]
        builders = [_catalog_anchor_builder]

        c1 = corpus_from_okg_cached(
            conn, generation_id=gen1,
            selectors=selectors, anchor_builders=builders,
        )
        c2 = corpus_from_okg_cached(
            conn, generation_id=gen2,
            selectors=selectors, anchor_builders=builders,
        )
        # Different generation_id keys → different cache entries.
        # (They MAY have equal content if the data didn't change,
        # but the dicts are distinct Python objects.)
        assert c1 is not c2

    def test_clear_cache_drops_entries(self, conn):
        gen = _insert_generation(conn, deployment_name="test")
        _apply_nodes(
            conn,
            [(
                "publication:a", "publication",
                {"pub_number": "A", "title": "P", "status": "current"},
            )],
        )
        _checkpoint(conn, gen)
        selectors = [by_type("publication")]
        builders = [_catalog_anchor_builder]

        first = corpus_from_okg_cached(
            conn, generation_id=gen,
            selectors=selectors, anchor_builders=builders,
        )
        clear_corpus_cache()
        second = corpus_from_okg_cached(
            conn, generation_id=gen,
            selectors=selectors, anchor_builders=builders,
        )
        # After clear, cache misses → different dict objects.
        assert second is not first
        # But the projections agree on content.
        assert set(first.keys()) == set(second.keys())

    def test_cache_speedup_at_least_10x(self, conn):
        # Stage 0 finding said the cache should be >=100x faster
        # than a cold call. Setting the gate at 10x here keeps
        # the test robust against small Postgres latency
        # variations on shared CI; 100x is verified informally.
        import time

        gen = _insert_generation(conn, deployment_name="test")
        # Insert ~50 nodes so the SQL query has nontrivial cost.
        _apply_nodes(
            conn,
            [
                (
                    f"publication:p{i}",
                    "publication",
                    {
                        "pub_number": f"P{i}",
                        "title": f"Pub {i}",
                        "status": "current",
                    },
                )
                for i in range(50)
            ],
        )
        _checkpoint(conn, gen)
        selectors = [by_type("publication")]
        builders = [_catalog_anchor_builder]

        # Cold call.
        clear_corpus_cache()
        t0 = time.perf_counter()
        first = corpus_from_okg_cached(
            conn, generation_id=gen,
            selectors=selectors, anchor_builders=builders,
        )
        cold_ms = (time.perf_counter() - t0) * 1000

        # Cached call.
        t0 = time.perf_counter()
        second = corpus_from_okg_cached(
            conn, generation_id=gen,
            selectors=selectors, anchor_builders=builders,
        )
        warm_ms = (time.perf_counter() - t0) * 1000

        assert second is first
        # Warm call should be at least 10x faster than cold.
        assert warm_ms * 10 < cold_ms, (
            f"cache too slow: cold={cold_ms:.3f}ms warm={warm_ms:.3f}ms"
        )
