"""MCP read-surface scaling + honest pagination (issue #627).

Overview/aggregate tools must never materialize unbounded row sets in
Python: under off/audit the counts are SQL aggregates (parity-tested
against the per-row path), and preview_advance filters only the
generation delta. Paged operators compute `truncated` from the pre-filter
fetch so Nomos-hidden rows cannot mislabel a page as complete.
"""

from __future__ import annotations

from typing import Any

import psycopg
import pytest

from okg.substrate.attributes import (
    materialize_effective_attributes_for_generation,
    record_attribute_observation,
)
from okg.substrate.embeddings import HashEmbedder
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import (
    aggregate_edge,
    expand_neighbors,
    expand_path,
    inspect_subtype,
    open_session,
    preview_advance,
)
from okg.substrate.mcp.search import search
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.substrate.mcp.test_nomos_source_lineage_mcp import (
    _activate_source_policy,
    _publish_lineage_graph,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _publish(dsn: str) -> str:
    result = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert result["status"] == "published"
    return str(result["generation_id"])


def _enable_enforcement(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-client")


class _CountingGraph:
    """Checkout proxy recording the row count of every query result."""

    def __init__(self, graph: Any, seen: list[int]) -> None:
        self._graph = graph
        self._seen = seen

    def query(self, sql: str, params: Any = None) -> Any:
        rows = self._graph.query(sql, params or {})
        self._seen.append(len(rows))
        return rows

    def __getattr__(self, name: str) -> Any:
        return getattr(self._graph, name)


def test_inspect_subtype_sql_summary_matches_per_row(
    clean_runtime: str,
) -> None:
    """Off-mode SQL aggregates (with overlay folding) must equal the exact
    per-row summary field-for-field, including seeded attribute overlays."""
    from okg.substrate.mcp import tools

    _publish_lineage_graph(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        # Overlay one visible node's attr so the SQL path must fold the
        # effective value (string -> number type change + a brand-new key).
        record_attribute_observation(
            conn,
            target_kind="node",
            target_id="node:public-docs",
            generation_id=session.generation_id,
            attr_key="text",
            value=42,
            producer="test:overlay",
        )
        record_attribute_observation(
            conn,
            target_kind="node",
            target_id="node:public-docs",
            generation_id=session.generation_id,
            attr_key="overlay_only_key",
            value=True,
            producer="test:overlay",
        )
        materialized = materialize_effective_attributes_for_generation(
            conn,
            generation_id=session.generation_id,
        )
        assert materialized["overlaid"] >= 1

        graph = tools._chronos_checkout(conn, session)
        sql_summary = tools._subtype_summary_sql(
            conn, session, graph, "page",
        )
        per_row_summary = tools._subtype_summary_per_row(
            conn, session, "page",
        )

    # Off-mode: the lineage filter is a pass-through, so both paths see the
    # same stored attrs (including the stored lineage attr) and must agree
    # exactly on freqs, types, counts, and routes.
    assert sql_summary["node_count"] == per_row_summary["node_count"]
    assert sql_summary["attr_keys"] == per_row_summary["attr_keys"]
    assert sql_summary["edges_out"] == per_row_summary["edges_out"]
    assert sql_summary["edges_in"] == per_row_summary["edges_in"]
    overlaid = {
        row["name"]: row
        for row in sql_summary["attr_keys"]
    }
    # The overlay-only key exists ONLY through the effective projection, so
    # its presence proves the SQL path folded overlays.
    assert overlaid["overlay_only_key"]["type"] == "boolean"
    assert overlaid["text"]["freq"] == 1.0


def test_aggregate_edge_sql_matches_per_row(clean_runtime: str) -> None:
    """Off-mode aggregate(edge) SQL GROUP BY must equal the legacy per-row
    grouping exactly."""
    from okg.substrate.mcp import tools

    _publish_lineage_graph(clean_runtime)
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        payload = aggregate_edge(conn, session)

        rows = tools._visible_edge_rows(conn, session)
        grouped: dict[tuple[str, str, str], int] = {}
        for row in rows:
            key = (
                str(row.get("src_subtype")),
                str(row.get("edge_type")),
                str(row.get("dst_subtype")),
            )
            grouped[key] = grouped.get(key, 0) + 1
        legacy = [
            {
                "src_subtype": key[0],
                "edge_type": key[1],
                "dst_subtype": key[2],
                "count": count,
            }
            for key, count in sorted(
                grouped.items(),
                key=lambda item: (-item[1], item[0]),
            )
        ]

    assert payload["routes"] == legacy
    assert payload["total_count"] == len(legacy)
    assert payload["truncated"] is False


def test_overview_tools_never_materialize_unbounded_rows(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On a Nomos-off deployment far above any budget, inspect(subtype) and
    aggregate(edge) must compute counts in SQL: no checkout query may return
    more than a bounded handful of rows."""
    from okg.substrate.mcp import tools

    n_nodes = 400
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    f"node:bulk-{i}",
                    "page",
                    {"classification": "internal", "text": f"bulk {i}"},
                    {"id": f"node:bulk-{i}"},
                    {"v": 1},
                )
                for i in range(n_nodes)
            ]
            + [
                EdgeFact(
                    f"node:bulk-{i}",
                    f"node:bulk-{i + 1}",
                    "references",
                    {"classification": "internal"},
                    source_record_id={"id": f"edge:bulk-{i}"},
                    source_revision={"v": 1},
                )
                for i in range(n_nodes - 1)
            ],
            source="public_docs",
        )
    _publish(clean_runtime)

    seen: list[int] = []
    real_checkout = tools._chronos_checkout
    monkeypatch.setattr(
        tools,
        "_chronos_checkout",
        lambda conn, session: _CountingGraph(
            real_checkout(conn, session), seen,
        ),
    )

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        payload = inspect_subtype(conn, session, "page")
        rollup = aggregate_edge(conn, session)

    assert payload["node_count"] == n_nodes
    assert rollup["routes"][0]["count"] == n_nodes - 1
    bound = 50  # group/sample cardinality, never the row population
    oversized = [count for count in seen if count > bound]
    assert not oversized, (
        f"an overview checkout query materialized {max(oversized)} rows; "
        f"counts must aggregate in SQL"
    )


def test_preview_advance_filters_only_the_delta(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Enforce-mode preview_advance must pass only delta rows (one-sided +
    content-changed, per pin) through the per-row filter, and reproduce the
    legacy full-materialization counts."""
    first_generation = _publish_lineage_graph(
        clean_runtime, include_restricted=False,
    )
    # Second generation: one modified public node (content change), one new
    # visible public node, plus a new node from the denied source (hidden).
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "node:public-docs",
                    "page",
                    {"classification": "internal", "text": "public docs v2"},
                    {"id": "node:public-docs"},
                    {"v": 2},
                ),
                NodeFact(
                    "node:public-third",
                    "page",
                    {"classification": "internal", "text": "public third"},
                    {"id": "node:public-third"},
                    {"v": 1},
                ),
            ],
            source="public_docs",
        )
        store.apply_facts(
            [
                NodeFact(
                    "node:google-drive",
                    "page",
                    {"classification": "internal", "text": "google docs"},
                    {"id": "node:google-drive"},
                    {"v": 1},
                ),
            ],
            source="google_drive",
        )
    _publish(clean_runtime)
    _activate_source_policy(clean_runtime)
    _enable_enforcement(monkeypatch)

    from okg.substrate.nomos import read_filter

    filtered_node_batches: list[int] = []
    filtered_edge_batches: list[int] = []
    real_node_filter = read_filter.filter_node_rows_for_session
    real_edge_filter = read_filter.filter_edge_rows_for_session

    def counting_node_filter(conn, session, rows, **kwargs):
        rows = list(rows)
        filtered_node_batches.append(len(rows))
        return real_node_filter(conn, session, rows, **kwargs)

    def counting_edge_filter(conn, session, rows, **kwargs):
        rows = list(rows)
        filtered_edge_batches.append(len(rows))
        return real_edge_filter(conn, session, rows, **kwargs)

    monkeypatch.setattr(
        read_filter, "filter_node_rows_for_session", counting_node_filter,
    )
    monkeypatch.setattr(
        read_filter, "filter_edge_rows_for_session", counting_edge_filter,
    )

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(
            conn,
            deployment_name="default",
            requested_generation=first_generation,
        )
        preview = preview_advance(conn, session, to="latest")

    # Delta: nodes = {public-docs (changed), public-third (new, visible),
    # google-drive (new, hidden)}; edges unchanged. Only the visible new
    # node counts as added; the changed node is visible on both sides and
    # cancels.
    assert preview["would_advance"] is True
    assert preview["added_node_count"] == 1
    assert preview["retracted_node_count"] == 0
    assert preview["added_edge_count"] == 0
    assert preview["retracted_edge_count"] == 0
    assert preview["truncated"] is False
    # from-side: only the changed node (1 row); to-side: changed + 2 new
    # (3 rows). Never the full graphs (4+ nodes per side).
    assert filtered_node_batches == [1, 3]
    assert filtered_edge_batches == [0, 0]


def test_truncated_computed_from_prefilter_fetch(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When Nomos hides rows inside the fetch window, every bounded-fetch
    surface must still report truncated=True (the pre-#627 post-filter
    computation silently mislabeled such pages as complete)."""
    generation = _publish_lineage_graph(clean_runtime)
    embedder = HashEmbedder()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Embeddings: the DENIED node ranks closest to the query, the two
        # public nodes follow — so the semantic fetch window (limit+1=2)
        # contains a hidden row.
        qvec = embedder.embed("query keyword docs")
        near = embedder.embed("query keyword docs drift")
        far = embedder.embed("totally orthogonal wording")
        from tests.substrate.test_search import _write_embedding

        _write_embedding(conn, "node:google-drive", qvec, generation)
        _write_embedding(conn, "node:public-docs", near, generation)
        _write_embedding(conn, "node:public-more", far, generation)
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "node:public-delta",
                    "page",
                    {"classification": "internal", "text": "publish delta"},
                    {"id": "node:public-delta"},
                    {"v": 1},
                )
            ],
            source="public_docs",
        )
    _publish(clean_runtime)
    _activate_source_policy(clean_runtime)
    _enable_enforcement(monkeypatch)

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        # expand(neighbors): public-docs has 2 outgoing references; the one
        # into google-drive is hidden. limit=1 fetches 2 raw rows.
        neighbors = expand_neighbors(
            conn,
            session,
            "node:public-docs",
            limit=1,
        )
        # expand(path): Nomos enforce cannot let a raw pre-filter window drive
        # truncation because doing so leaks hidden structure. It returns one
        # typed visibility-limited outcome; one-hop neighbors below retain the
        # established page contract.
        path = expand_path(
            conn,
            session,
            "node:public-docs",
            hops=[("references", "out")],
            limit=1,
        )
        # search(semantic): hidden node ranks first inside the window.
        semantic = search(
            conn,
            session,
            "query keyword docs",
            method="semantic",
            embedder=embedder,
            limit=1,
        )

    assert [row["dst"] for row in neighbors["neighbors"]] == ["node:public-more"]
    assert neighbors["truncated"] is True

    assert path["results"] == []
    assert path["outcome"]["code"] == "visibility_limited"
    assert path["truncated"] is False

    assert [row["node_id"] for row in semantic["results"]] == ["node:public-docs"]
    assert semantic["truncated"] is True

    # Operator surfaces (server filter/map): the graph holds 5 'page'
    # nodes, 3 of them hidden (google-drive, mixed-derived need the denied
    # source). A limit=1 page fetches 2 raw rows; whatever the window
    # ordering, more raw rows exist, so truncated MUST be True — even when
    # hiding empties or under-fills the page (the old post-filter
    # computation reported False whenever ≤1 row survived).
    from okg.substrate.mcp.server import build_server

    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        filter_page = mcp._tool_manager._tools["filter"].fn(
            input={"kind": "nodes"},
            predicate={},
            select=["node_id"],
            limit=1,
        )
        map_page = mcp._tool_manager._tools["map"].fn(
            input={"kind": "nodes"},
            projection=["node_id"],
            limit=1,
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    hidden = {"node:google-drive", "node:mixed-derived"}
    assert filter_page["truncated"] is True
    assert not hidden & {row["node_id"] for row in filter_page["items"]}
    assert map_page["truncated"] is True
    assert not hidden & {row["node_id"] for row in map_page["items"]}


def test_operator_pages_never_overlap_under_hiding(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Consecutive filter pages under enforce hiding must not serve the same
    row twice: each page serves only ITS raw window (the +1 lookahead row
    belongs to the next page)."""
    _publish_lineage_graph(clean_runtime)
    _activate_source_policy(clean_runtime)
    _enable_enforcement(monkeypatch)

    from okg.substrate.mcp.server import build_server

    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        tool = mcp._tool_manager._tools["filter"]
        pages = []
        offset = 0
        for _ in range(10):
            page = tool.fn(
                input={"kind": "nodes"},
                predicate={},
                select=["node_id"],
                limit=1,
                offset=offset,
            )
            pages.append(page)
            if not page["truncated"]:
                break
            offset = page["next_offset"]
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    served = [row["node_id"] for page in pages for row in page["items"]]
    assert len(served) == len(set(served)), (
        f"paging served a row twice: {served}"
    )
    # Iterating raw windows must surface every visible row exactly once.
    assert set(served) == {"node:public-docs", "node:public-more"}


def test_overview_probe_failures_raise_not_fallback(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Enforce subtype inspection never runs a raw cardinality probe; other
    overview SQL failures still raise instead of silently falling back."""
    from okg.substrate.mcp import tools
    from okg.substrate.mcp import session as session_mod

    _publish_lineage_graph(clean_runtime)
    _activate_source_policy(clean_runtime)
    _enable_enforcement(monkeypatch)

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")

        def _boom(*_args: Any, **_kwargs: Any) -> int:
            raise RuntimeError("probe failed")

        monkeypatch.setattr(tools, "_subtype_total_rows", _boom)
        declared = inspect_subtype(conn, session, "page")
        assert declared["node_count"] is None
        assert declared["visibility"]["status"] == "observed_statistics_withheld"

        class _BrokenGraph:
            def query(self, *_args: Any, **_kwargs: Any) -> Any:
                raise RuntimeError("aggregate query failed")

        monkeypatch.setattr(
            tools,
            "_chronos_checkout",
            lambda *_a, **_k: _BrokenGraph(),
        )
        with pytest.raises(RuntimeError, match="aggregate query failed"):
            aggregate_edge(conn, session)
        monkeypatch.undo()

        _enable_enforcement(monkeypatch)
        monkeypatch.setattr(
            session_mod,
            "_ROW_CONTENT_HASH_SQL",
            "md5(this_is_not_a_column::text)",
        )
        with pytest.raises(Exception, match="this_is_not_a_column"):
            preview_advance(conn, session, to="latest")
