from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path

import psycopg
import pytest


from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore, _edge_id
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.docsite import DocsiteSource
from okg.substrate.mcp import open_session
from okg.substrate.mcp.run_sql import run_sql
from okg.substrate.mcp.search import search
from okg.substrate.nomos.source_lineage import LINEAGE_ATTR
from okg.substrate.publisher import Publisher
from okg.viz import reads as viz_reads
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


@pytest.fixture
def chronos_graph_dsn(clean_runtime: str) -> str:
    """The migrated substrate test database these store tests run against.

    This fixture used to start its OWN `postgres:16-alpine` container on a
    hardcoded port and `pytest.skip()` when that failed. That was wrong twice
    over: the vanilla image has no `bm25` access method, so the schema
    bootstrap these tests exercise raised `access method "bm25" does not
    exist` on any developer machine that ran it, and on CI the docker start
    failed outright, so all thirteen tests silently skipped and the store
    behaviour they cover was verified NOWHERE. Delegating to `clean_runtime`
    puts them on the same migrated database as the rest of the substrate
    suite -- the database the lane already provisions with the required
    extensions -- so they run for real and fail loudly when Postgres is
    missing.
    """
    return clean_runtime


def _reset_chronos_graph(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        had_okg_views = (
            cur.execute("SELECT to_regclass('okg.v_nodes')").fetchone()[0]
            is not None
        )
        cur.execute("DROP TABLE IF EXISTS okg.graph_edges CASCADE")
        cur.execute("DROP TABLE IF EXISTS okg.graph_nodes CASCADE")
        cur.execute(
            """
            DO $$
            DECLARE rec RECORD;
            BEGIN
              FOR rec IN
                SELECT schemaname, tablename
                  FROM pg_tables
                 WHERE schemaname = 'public'
                   AND (
                     tablename LIKE '_chronos_branch_%'
                     OR tablename LIKE '_chronos_b_%'
                   )
              LOOP
                EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE',
                               rec.schemaname, rec.tablename);
              END LOOP;
            END
            $$;
            """
        )
        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.graph_store_internal.common import (
            clear_schema_ensured_cache,
        )
        from okg.substrate.graph_store_internal.schema import (
            _clear_mcp_grant_guard,
        )
        from chronos_core.branching._common import clear_all_registry_caches

        # The DROPs above removed the schema/tables out-of-band, so drop the
        # process-level caches/guards before reconstructing — otherwise the
        # schema-bootstrap guard would skip ensure_schema() (leaving
        # okg.graph_nodes/edges uncreated), the registry cache would report the
        # dropped tables as still registered, and the grant guard would skip the
        # re-created _chronos* tables.
        clear_all_registry_caches()
        clear_schema_ensured_cache()
        _clear_mcp_grant_guard()
        ChronosGraphStore.from_connection(conn, ensure_schema=True)
    if had_okg_views:
        from okg.substrate.db.migrate import install_views

        install_views(dsn)


def _chronos_interval_index_defs_by_logical(dsn: str) -> dict[str, str]:
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            """
            SELECT table_name, physical_table
              FROM _chronos_branch_tables
             WHERE table_name IN ('okg.graph_nodes', 'okg.graph_edges')
               AND backend = 'interval'
            """
        ).fetchall()
        physical_by_logical = {
            str(table_name): str(physical_table)
            for table_name, physical_table in rows
        }
        index_defs: dict[str, str] = {}
        for physical_table in physical_by_logical.values():
            defs = conn.execute(
                """
                SELECT indexdef
                  FROM pg_indexes
                 WHERE schemaname = 'public'
                   AND tablename = %s
                 ORDER BY indexname
                """,
                (physical_table,),
            ).fetchall()
            index_defs[physical_table] = "\n".join(
                str(row[0]) for row in defs
            )
    return {
        logical: index_defs[physical]
        for logical, physical in physical_by_logical.items()
    }


def _assert_chronos_source_read_indexes(index_defs: dict[str, str]) -> None:
    node_defs = index_defs["okg.graph_nodes"]
    edge_defs = index_defs["okg.graph_edges"]
    assert "(node_id, live_lo, live_hi)" in node_defs
    assert "(source, node_id, live_lo, live_hi)" in node_defs
    assert "(source, run_id, live_lo, live_hi, node_id)" in node_defs
    assert "(subtype, ((attrs ->> 'path'::text)), live_lo, live_hi)" in node_defs
    assert (
        "(subtype, ((attrs ->> 'file_path'::text)), live_lo, live_hi)"
        in node_defs
    )
    semantic_attrs = (
        "semantic_key",
        "canonical_name",
        "target_semantic_key",
        "semantic_target_key",
        "symbol_semantic_key",
        "code_symbol_semantic_key",
        "handler_semantic_key",
        "function_semantic_key",
        "source_semantic_key",
        "target_canonical_name",
        "symbol_canonical_name",
        "code_symbol_canonical_name",
        "handler_canonical_name",
        "function_canonical_name",
        "source_canonical_name",
    )
    for attr in semantic_attrs:
        assert (
            f"(subtype, ((attrs ->> '{attr}'::text)), live_lo, live_hi)"
            in node_defs
        )
    assert "(source, edge_id, live_lo, live_hi)" in edge_defs
    assert "(src, edge_type, live_lo, live_hi, dst)" in edge_defs
    assert "(dst, live_lo, live_hi, edge_id)" in edge_defs
    assert "(source, dst, live_lo, live_hi, edge_id)" in edge_defs
    assert "(source, run_id, live_lo, live_hi, edge_id)" in edge_defs
    # okg#3112 step 1 retired these (cold_history.
    # RETIRED_INTERVAL_SOURCE_READ_INDEX_SUFFIXES): src = ? runs on the
    # identity index above, record-set reads order by edge_id, and the run
    # index no longer carries the src/edge_type/dst text keys.
    assert "(src, live_lo, live_hi, edge_id)" not in edge_defs
    assert "(source, src, edge_type, dst, live_lo, live_hi)" not in edge_defs
    assert "(source, run_id, live_lo, live_hi, src, edge_type, dst)" not in edge_defs
    assert "(live_lo) WHERE" not in node_defs and "(live_hi) WHERE" not in node_defs


def _assert_node_attrs_with_lineage(
    row: dict[str, object],
    *,
    text: str,
    source_id: str,
) -> None:
    attrs = row["attrs"]
    assert isinstance(attrs, dict)
    assert attrs["text"] == text
    lineage = attrs[LINEAGE_ATTR]
    assert isinstance(lineage, dict)
    assert source_id in lineage["source_ids"]


def test_chronos_graph_store_ensures_source_read_indexes(
    chronos_graph_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The node attrs->>'path' / ->>'file_path' expression indexes are opt-in
    # (off by default to spare path-less deployments the write overhead); enable
    # them so this test exercises the full ensured index set.
    monkeypatch.setenv("OKG_CHRONOS_NODE_ATTR_PATH_INDEXES", "1")
    _reset_chronos_graph(chronos_graph_dsn)
    store = ChronosGraphStore.connect(chronos_graph_dsn)
    try:
        _assert_chronos_source_read_indexes(
            _chronos_interval_index_defs_by_logical(chronos_graph_dsn)
        )
    finally:
        store.close()


def test_chronos_graph_store_ensures_source_read_indexes_on_clean_runtime(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_CHRONOS_NODE_ATTR_PATH_INDEXES", "1")
    _reset_chronos_graph(clean_runtime)
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _assert_chronos_source_read_indexes(
            _chronos_interval_index_defs_by_logical(clean_runtime)
        )
    finally:
        store.close()


def test_node_attr_path_index_allowlist_parsing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.graph_store_internal.schema import (
        _node_attr_path_index_allowlist,
    )

    for all_value in ("1", "true", " ALL ", "On"):  # all (case/space-insensitive)
        monkeypatch.setenv("OKG_CHRONOS_NODE_ATTR_PATH_INDEXES", all_value)
        assert _node_attr_path_index_allowlist() is None
    for none_value in ("0", "", "OFF", "false"):  # none
        monkeypatch.setenv("OKG_CHRONOS_NODE_ATTR_PATH_INDEXES", none_value)
        assert _node_attr_path_index_allowlist() == set()
    # comma list: whitespace trimmed, empty tokens dropped, lowercased so a
    # case-typo still matches the lowercase index suffixes.
    monkeypatch.setenv(
        "OKG_CHRONOS_NODE_ATTR_PATH_INDEXES",
        "Node_Attr_Source_CN_Visible, ,node_attr_semantic_target_sk_vis",
    )
    assert _node_attr_path_index_allowlist() == {
        "node_attr_source_cn_visible",
        "node_attr_semantic_target_sk_vis",
    }


def test_chronos_graph_store_attr_path_index_allowlist(
    chronos_graph_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The allowlist form creates ONLY the listed attr-path indexes and drops the
    # rest, so a deployment indexes only the attr paths its workload queries
    # (PACT bound-okg-workspace-disk-growth task 2). Keeps the hot paths, sheds
    # the cold ones, and the choice sticks across ensure_schema.
    monkeypatch.setenv(
        "OKG_CHRONOS_NODE_ATTR_PATH_INDEXES",
        "node_attr_source_cn_visible,node_attr_semantic_target_sk_vis",
    )
    _reset_chronos_graph(chronos_graph_dsn)
    store = ChronosGraphStore.connect(chronos_graph_dsn)
    try:
        node_defs = _chronos_interval_index_defs_by_logical(
            chronos_graph_dsn
        )["okg.graph_nodes"]
    finally:
        store.close()
    # listed -> present
    assert "'source_canonical_name'::text" in node_defs
    assert "'semantic_target_key'::text" in node_defs
    # not listed -> dropped/absent
    assert "'function_canonical_name'::text" not in node_defs
    assert "'handler_canonical_name'::text" not in node_defs


def test_chronos_graph_store_applies_edits_to_default_branch(
    chronos_graph_dsn: str,
) -> None:
    _reset_chronos_graph(chronos_graph_dsn)
    store = ChronosGraphStore.connect(chronos_graph_dsn)
    try:
        result = store.apply_facts([
            NodeFact(
                node_id="n:1",
                subtype="claim",
                attrs={"text": "hello"},
                source_record_id={"id": "1"},
                source_revision={"rev": 1},
            ),
            NodeFact(
                node_id="n:2",
                subtype="evidence",
                attrs={"text": "world"},
                source_record_id={"id": "2"},
                source_revision={"rev": 1},
            ),
            EdgeFact(src="n:1", dst="n:2", edge_type="supported_by"),
        ])

        assert result.nodes_upserted == 2
        assert result.edges_upserted == 1
        assert store.query(
            """
            SELECT node_id, subtype, attrs
            FROM okg.graph_nodes
            ORDER BY node_id
            """
        ) == [
            {"node_id": "n:1", "subtype": "claim", "attrs": {"text": "hello"}},
            {"node_id": "n:2", "subtype": "evidence", "attrs": {"text": "world"}},
        ]
        assert store.query(
            "SELECT src, dst, edge_type FROM okg.graph_edges"
        ) == [{
            "src": "n:1",
            "dst": "n:2",
            "edge_type": "supported_by",
        }]
    finally:
        store.close()


def test_checkout_generation_zero_reads_working_head(
    chronos_graph_dsn: str,
) -> None:
    """`checkout_generation(0)` is the publisher's pre-checkpoint sentinel for
    the current working generation.

    The extractor / lens-promotion / enricher stages run BEFORE the generation
    is checkpointed and are invoked with a hardcoded ``generation_id=0``. That
    must resolve to the working branch head (source facts applied, not yet
    checkpointed) rather than a literal ``generation:0`` checkpoint that never
    exists. Regression for the first-publish structural-enricher crash
    (``KeyError: Chronos checkpoint 'generation:0' not found``) that silently
    blocked all paper decomposition on a deployment's very first publish.
    """
    _reset_chronos_graph(chronos_graph_dsn)
    store = ChronosGraphStore.connect(chronos_graph_dsn)
    try:
        store.apply_facts([
            NodeFact(
                node_id="paper:z",
                subtype="paper",
                attrs={"source_pdf_path": "/x.pdf"},
                source_record_id={"id": "paper:z"},
                source_revision={"rev": 1},
            ),
        ])
        # Pre-checkpoint: the sentinel resolves to the working head and sees the
        # just-applied paper (previously this raised KeyError 'generation:0').
        working = store.checkout_generation(0)
        assert [
            row["node_id"]
            for row in working.query(
                "SELECT node_id FROM okg.graph_nodes WHERE subtype = 'paper'"
            )
        ] == ["paper:z"]

        # A concrete committed generation id still resolves through the
        # checkpoint store -- the sentinel handling must not shadow real
        # generations (checkpoint_generation returns a string checkpoint id).
        gen = store.checkpoint_generation(deployment_name="reftest", branch="default")
        assert str(gen) != "0"
        committed = store.checkout_generation(gen)
        assert [
            row["node_id"]
            for row in committed.query(
                "SELECT node_id FROM okg.graph_nodes WHERE subtype = 'paper'"
            )
        ] == ["paper:z"]
    finally:
        store.close()


def test_chronos_graph_store_collapses_duplicate_upserts_in_one_flush(
    chronos_graph_dsn: str,
) -> None:
    _reset_chronos_graph(chronos_graph_dsn)
    store = ChronosGraphStore.connect(chronos_graph_dsn)
    try:
        result = store.apply_facts([
            NodeFact(
                node_id="n:dup",
                subtype="claim",
                attrs={"text": "old"},
                source_record_id={"id": "1"},
                source_revision={"rev": 1},
            ),
            NodeFact(
                node_id="n:dup",
                subtype="claim",
                attrs={"text": "new"},
                source_record_id={"id": "1"},
                source_revision={"rev": 2},
            ),
        ])

        assert result.nodes_upserted == 1
        assert store.query(
            """
            SELECT node_id, attrs
            FROM okg.graph_nodes
            WHERE node_id = 'n:dup'
            """
        ) == [{"node_id": "n:dup", "attrs": {"text": "new"}}]
    finally:
        store.close()


def test_chronos_graph_store_same_flush_retraction_then_insert_leaves_row_live(
    chronos_graph_dsn: str,
) -> None:
    _reset_chronos_graph(chronos_graph_dsn)
    store = ChronosGraphStore.connect(chronos_graph_dsn)
    try:
        store.apply_facts([
            NodeFact("n:1", "claim", {"text": "old"}, {}, {}),
            NodeFact("n:2", "evidence", {"text": "endpoint"}, {}, {}),
            EdgeFact("n:1", "n:2", "supported_by", attrs={"rev": 1}),
        ])

        result = store.apply_facts([
            NodeFact("n:1", "claim", {}, {}, {}, op="R"),
            EdgeFact("n:1", "n:2", "supported_by", op="R"),
            NodeFact("n:1", "claim", {"text": "new"}, {}, {}),
            EdgeFact("n:1", "n:2", "supported_by", attrs={"rev": 2}),
        ])

        assert result.nodes_upserted == 1
        assert result.nodes_deleted == 1
        assert result.edges_upserted == 1
        assert store.query(
            """
            SELECT node_id, attrs
            FROM okg.graph_nodes
            WHERE node_id = 'n:1'
            """
        ) == [{"node_id": "n:1", "attrs": {"text": "new"}}]
        assert store.query(
            """
            SELECT src, dst, edge_type, attrs
            FROM okg.graph_edges
            WHERE src = 'n:1' AND dst = 'n:2'
            """
        ) == [{
            "src": "n:1",
            "dst": "n:2",
            "edge_type": "supported_by",
            "attrs": {"rev": 2},
        }]
    finally:
        store.close()


def test_chronos_graph_store_source_retraction_does_not_delete_other_source_edge(
    chronos_graph_dsn: str,
) -> None:
    _reset_chronos_graph(chronos_graph_dsn)
    store = ChronosGraphStore.connect(chronos_graph_dsn)
    try:
        store.apply_facts(
            [
                NodeFact("n:1", "claim", {"text": "claim"}, {}, {}),
                NodeFact("n:2", "evidence", {"text": "evidence"}, {}, {}),
            ],
            source="source-a",
        )
        store.apply_facts(
            [EdgeFact("n:1", "n:2", "supported_by")],
            source="source-b",
        )

        result = store.apply_facts(
            [EdgeFact("n:1", "n:2", "supported_by", op="R")],
            source="source-a",
        )

        assert result.edges_deleted == 0
        assert store.query(
            """
            SELECT src, edge_type, dst, source
              FROM okg.graph_edges
             WHERE src = 'n:1' AND dst = 'n:2'
            """
        ) == [{
            "src": "n:1",
            "edge_type": "supported_by",
            "dst": "n:2",
            "source": "source-b",
        }]
    finally:
        store.close()


def test_retract_orphaned_derived_edges_tombstones_cross_source_dangling(
    chronos_graph_dsn: str,
) -> None:
    """A DERIVED edge orphaned by a re-ingest -- its endpoint node retracted -- is
    tombstoned by orphan-retraction, even though it was produced by a DIFFERENT
    source than the one that retracted the endpoint. That cross-source case is
    exactly what the source-scoped incident-edge cleanup misses (it leaves the edge
    dangling and the no-dangling gate blocks). Unrelated live edges stay live, the
    retraction is as-of (a Chronos tombstone), and a second pass is a no-op.
    """
    _reset_chronos_graph(chronos_graph_dsn)
    store = ChronosGraphStore.connect(chronos_graph_dsn)
    try:
        # Primary ingest (source-a): a document + two entities.
        store.apply_facts(
            [
                NodeFact("doc:1", "claim", {"text": "doc one"}, {}, {}),
                NodeFact("ent:a", "evidence", {"text": "Alice"}, {}, {}),
                NodeFact("ent:b", "evidence", {"text": "Bob"}, {}, {}),
            ],
            source="source-a",
        )
        # Enricher output (a DIFFERENT source): a derived edge into the document and
        # an unrelated derived edge between two still-live entities.
        store.apply_facts(
            [
                EdgeFact("doc:1", "ent:a", "supported_by", provenance="derived_deterministic"),
                EdgeFact("ent:a", "ent:b", "supported_by", provenance="derived_deterministic"),
            ],
            source="_publisher_derived",
        )

        # The re-ingest retracts the document (source-scoped). source-a's own incident
        # edges go, but the cross-source derived edge survives -> dangling.
        store.apply_facts(
            [NodeFact("doc:1", "claim", {}, {}, {}, op="R")],
            source="source-a",
        )
        assert store.query(
            "SELECT edge_type FROM okg.graph_edges WHERE src='doc:1' AND dst='ent:a'"
        ) == [{"edge_type": "supported_by"}], "precondition: the derived edge dangles"

        retracted = store.retract_orphaned_derived_edges()
        assert retracted == 1

        # The dangling derived edge is now not-live...
        assert store.query(
            "SELECT edge_type FROM okg.graph_edges WHERE src='doc:1' AND dst='ent:a'"
        ) == []
        # ...the live entities + the unrelated derived edge persist...
        assert store.query(
            "SELECT edge_type FROM okg.graph_edges WHERE src='ent:a' AND dst='ent:b'"
        ) == [{"edge_type": "supported_by"}]
        assert {
            r["node_id"] for r in store.query("SELECT node_id FROM okg.graph_nodes")
        } == {"ent:a", "ent:b"}
        # ...and a second pass finds nothing left orphaned.
        assert store.retract_orphaned_derived_edges() == 0
    finally:
        store.close()


def test_retract_orphaned_derived_edges_leaves_explicit_dangling_to_policy(
    chronos_graph_dsn: str,
) -> None:
    """Orphan-retraction touches only DERIVED edges. An EXPLICIT edge dangling to a
    retracted endpoint is a source assertion (a forward reference) governed by the
    deployment's no-dangling policy -- it must NOT be silently retracted here."""
    _reset_chronos_graph(chronos_graph_dsn)
    store = ChronosGraphStore.connect(chronos_graph_dsn)
    try:
        store.apply_facts(
            [
                NodeFact("n:1", "claim", {"text": "a"}, {}, {}),
                NodeFact("n:2", "evidence", {"text": "b"}, {}, {}),
            ],
            source="source-a",
        )
        # An EXPLICIT (default provenance) edge from a second source into n:1.
        store.apply_facts(
            [EdgeFact("n:2", "n:1", "supported_by")],
            source="source-b",
        )
        store.apply_facts(
            [NodeFact("n:1", "claim", {}, {}, {}, op="R")],
            source="source-a",
        )

        # n:1 is gone, so n:2 -> n:1 dangles, but it is EXPLICIT -> not an orphan.
        assert store.retract_orphaned_derived_edges() == 0
        assert store.query(
            "SELECT edge_type FROM okg.graph_edges WHERE src='n:2' AND dst='n:1'"
        ) == [{"edge_type": "supported_by"}]
    finally:
        store.close()


def test_retract_orphaned_derived_edges_uses_enricher_ownership(
    clean_runtime: str,
) -> None:
    """Producer ownership wins over legacy semantic provenance labels."""
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts(
            [
                NodeFact("doc:owned", "claim", {}, {}, {}),
                NodeFact("evidence:a", "evidence", {}, {}, {}),
                NodeFact("evidence:b", "evidence", {}, {}, {}),
            ],
            source="source-a",
        )
        store.apply_facts(
            [
                EdgeFact(
                    "doc:owned",
                    "evidence:a",
                    "supported_by",
                    provenance="explicit",
                ),
                EdgeFact(
                    "doc:owned",
                    "evidence:b",
                    "supported_by",
                    provenance="extracted_model",
                ),
                EdgeFact(
                    "evidence:a",
                    "evidence:b",
                    "supported_by",
                    provenance="explicit",
                ),
            ],
            source="_enricher:fixture",
        )
        store.apply_facts(
            [
                EdgeFact(
                    "evidence:a",
                    "doc:owned",
                    "supported_by",
                    provenance="derived_deterministic",
                )
            ],
            source="_publisher_derived",
        )
        store.apply_facts(
            [
                EdgeFact(
                    "evidence:b",
                    "doc:owned",
                    "supported_by",
                    provenance="explicit",
                )
            ],
            source="source-b",
        )
        store.apply_facts(
            [
                EdgeFact(
                    "evidence:a",
                    "doc:owned",
                    "contrasts",
                    provenance="explicit",
                )
            ],
            source="xenricher:fixture",
        )
        store.apply_facts(
            [NodeFact("doc:owned", "claim", {}, {}, {}, op="R")],
            source="source-a",
        )

        assert store.retract_orphaned_derived_edges() == 3
        rows = store.query(
            "SELECT src, dst, source FROM okg.graph_edges ORDER BY source, src"
        )
        assert rows == [
            {
                "src": "evidence:a",
                "dst": "evidence:b",
                "source": "_enricher:fixture",
            },
            {
                "src": "evidence:b",
                "dst": "doc:owned",
                "source": "source-b",
            },
            {
                "src": "evidence:a",
                "dst": "doc:owned",
                "source": "xenricher:fixture",
            },
        ]
    finally:
        store.close()


def test_chronos_graph_store_rejects_missing_edge_endpoints(
    chronos_graph_dsn: str,
) -> None:
    _reset_chronos_graph(chronos_graph_dsn)
    store = ChronosGraphStore.connect(chronos_graph_dsn)
    try:
        with pytest.raises(SubstrateError, match="missing endpoint"):
            store.apply_facts([
                NodeFact("n:1", "claim", {"text": "hello"}, {}, {}),
                EdgeFact("n:1", "n:missing", "supported_by"),
            ])
        assert store.query("SELECT count(*) AS n FROM okg.graph_edges") == [{"n": 0}]
    finally:
        store.close()


def test_error_inside_caller_owned_transaction_keeps_original_error(
    chronos_graph_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #2159, the source-retirement shape.

    ``source_retirement._apply_plan`` builds the store with ``autocommit=False``
    and applies facts inside ``with conn.transaction():``, so ``apply_facts``
    opens a root ``BranchSession.transaction()`` inside a psycopg Transaction
    block. An error there triggers the session's rollback, and psycopg refuses
    an explicit ``rollback()`` inside that block. The caller must still see the
    original error, and the outer block must still undo the write.
    """

    class _WriteFailed(RuntimeError):
        pass

    _reset_chronos_graph(chronos_graph_dsn)
    with psycopg.connect(chronos_graph_dsn) as conn:
        store = ChronosGraphStore.from_connection(
            conn, ensure_schema=False, autocommit=False
        )

        def fail_inside_session_transaction(_branch: str) -> None:
            raise _WriteFailed("write failed inside the session transaction")

        # The first statement inside `with session.transaction():` in writes.py.
        monkeypatch.setattr(store, "_lock_branch_write", fail_inside_session_transaction)
        with pytest.raises(Exception) as raised:
            with conn.transaction():
                store.apply_facts([
                    NodeFact("n:retire", "claim", {"text": "x"}, {"id": "1"}, {"rev": 1}),
                ])

    assert type(raised.value) is _WriteFailed, (
        f"surfaced {type(raised.value).__name__}: {raised.value}"
    )
    assert str(raised.value) == "write failed inside the session transaction"
    assert any(
        "rollback after this error also failed: ProgrammingError" in note
        and "Explicit rollback() forbidden within a Transaction context" in note
        for note in getattr(raised.value, "__notes__", [])
    )
    check = ChronosGraphStore.connect(chronos_graph_dsn)
    try:
        assert check.query(
            "SELECT count(*) AS n FROM okg.graph_nodes WHERE node_id = 'n:retire'"
        ) == [{"n": 0}]
    finally:
        check.close()


def test_chronos_graph_store_isolates_branches_and_deletes_incident_edges(
    chronos_graph_dsn: str,
) -> None:
    _reset_chronos_graph(chronos_graph_dsn)
    store = ChronosGraphStore.connect(chronos_graph_dsn)
    try:
        store.apply_facts([
            NodeFact("n:1", "claim", {"text": "base"}, {}, {}),
            NodeFact("n:2", "evidence", {"text": "base evidence"}, {}, {}),
            EdgeFact("n:1", "n:2", "supported_by"),
        ])
        pinned_default = store.checkout("default")

        store.create_branch("experiment")
        result = store.apply_facts([
            NodeFact("n:1", "claim", {"text": "experiment"}, {}, {}),
            NodeFact("n:2", "evidence", {}, {}, {}, op="R"),
        ], branch="experiment")
        assert result.nodes_deleted == 1
        assert result.edges_deleted == 1
        assert result.node_attr_changes_by_subtype == {
            "claim": {"n:1": ("text",)},
            "evidence": {"n:2": ("__deleted__", "text")},
        }
        assert result.edges_by_type == {
            "supported_by": (_edge_id("n:1", "supported_by", "n:2"),),
        }

        assert store.query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'n:1'",
            branch="default",
        ) == [{"attrs": {"text": "base"}}]
        assert store.query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'n:1'",
            branch="experiment",
        ) == [{"attrs": {"text": "experiment"}}]
        assert store.query(
            "SELECT count(*) AS n FROM okg.graph_edges",
            branch="experiment",
        ) == [{"n": 0}]
        assert pinned_default.query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'n:1'"
        ) == [{"attrs": {"text": "base"}}]
    finally:
        store.close()


def test_generation_checkpoint_snapshots_default_branch(
    chronos_graph_dsn: str,
) -> None:
    _reset_chronos_graph(chronos_graph_dsn)
    store = ChronosGraphStore.connect(chronos_graph_dsn)
    try:
        store.apply_facts([
            NodeFact("n:1", "claim", {"text": "generation one"}, {}, {}),
        ])
        store.checkpoint_generation(1)

        store.apply_facts([
            NodeFact("n:1", "claim", {"text": "generation two"}, {}, {}),
            NodeFact("n:2", "claim", {"text": "new in two"}, {}, {}),
        ])

        gen_one = store.checkout_generation(1)
        assert gen_one.current_ref != store.checkout().current_ref
        assert gen_one.query(
            """
            SELECT node_id, attrs
              FROM okg.graph_nodes
             ORDER BY node_id
            """
        ) == [{
            "node_id": "n:1",
            "attrs": {"text": "generation one"},
        }]
        assert store.query(
            """
            SELECT node_id, attrs
              FROM okg.graph_nodes
             ORDER BY node_id
            """
        ) == [
            {"node_id": "n:1", "attrs": {"text": "generation two"}},
            {"node_id": "n:2", "attrs": {"text": "new in two"}},
        ]
    finally:
        store.close()


def test_source_runner_writes_chronos_graph_by_default(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    _reset_chronos_graph(clean_runtime)
    fixture = tmp_path / "fixture.json"
    fixture.write_text(
        """
        {
          "nodes": [
            {
              "node_id": "doc:1",
              "subtype": "document",
              "attrs": {"text": "one"}
            },
            {
              "node_id": "doc:2",
              "subtype": "document",
              "attrs": {"text": "two"}
            }
          ],
          "edges": [
            {"src": "doc:1", "dst": "doc:2", "edge_type": "links_to"}
          ]
        }
        """
    )

    runner = SourceRunner(clean_runtime, source_name="chronos_docs")
    runner.run(DocsiteSource(fixture_path=str(fixture)), mode="scope_complete")

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        rows = store.query(
            """
            SELECT node_id, subtype, attrs
            FROM okg.graph_nodes
            ORDER BY node_id
            """
        )
        assert [
            {"node_id": row["node_id"], "subtype": row["subtype"]}
            for row in rows
        ] == [
            {"node_id": "doc:1", "subtype": "document"},
            {"node_id": "doc:2", "subtype": "document"},
        ]
        _assert_node_attrs_with_lineage(
            rows[0],
            text="one",
            source_id="chronos_docs",
        )
        _assert_node_attrs_with_lineage(
            rows[1],
            text="two",
            source_id="chronos_docs",
        )
        assert store.query(
            "SELECT src, dst, edge_type FROM okg.graph_edges"
        ) == [{
            "src": "doc:1",
            "dst": "doc:2",
            "edge_type": "links_to",
        }]
    finally:
        store.close()


def test_publisher_snapshots_chronos_branch_as_generation(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    _reset_chronos_graph(clean_runtime)
    fixture = tmp_path / "fixture.json"
    fixture.write_text(
        """
        {
          "nodes": [
            {
              "node_id": "pub:1",
              "subtype": "document",
              "attrs": {"text": "publisher"}
            }
          ],
          "edges": []
        }
        """
    )

    SourceRunner(
        clean_runtime,
        source_name="publisher_sync",
    ).run(DocsiteSource(fixture_path=str(fixture)), mode="scope_complete")

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published"
    assert isinstance(out["generation_id"], str)

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.graph_generations WHERE generation_id > 0")
        assert cur.fetchone()[0] == 0

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        checkpoint = store.get_generation(out["generation_id"])
        assert checkpoint.metadata["kind"] == "okg_generation"
        assert checkpoint.metadata["status"] == "published"
        assert checkpoint.metadata["publisher"] == "chronos_native"
        generation = store.checkout_generation(out["generation_id"])
        rows = generation.query(
            """
            SELECT node_id, subtype, attrs
            FROM okg.graph_nodes
            WHERE node_id = 'pub:1'
            """
        )
        assert [
            {"node_id": row["node_id"], "subtype": row["subtype"]}
            for row in rows
        ] == [{"node_id": "pub:1", "subtype": "document"}]
        _assert_node_attrs_with_lineage(
            rows[0],
            text="publisher",
            source_id="publisher_sync",
        )
    finally:
        store.close()


def test_mcp_session_and_run_sql_read_chronos_generation(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    _reset_chronos_graph(clean_runtime)
    fixture = tmp_path / "fixture.json"
    fixture.write_text(
        """
        {
          "nodes": [
            {
              "node_id": "mcp:1",
              "subtype": "document",
              "attrs": {"text": "pinned"}
            }
          ],
          "edges": []
        }
        """
    )

    SourceRunner(
        clean_runtime,
        source_name="mcp_chronos",
    ).run(DocsiteSource(fixture_path=str(fixture)), mode="scope_complete")
    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.node_facts")
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT count(*) FROM okg.edge_facts")
            assert cur.fetchone()[0] == 0
            cur.execute(
                "SELECT count(*) FROM okg.graph_generations WHERE generation_id > 0"
            )
            assert cur.fetchone()[0] == 0
        session = open_session(conn, deployment_name="default")
        assert session.generation_id == out["generation_id"]
        payload = run_sql(
            conn,
            session,
            "SELECT node_id, subtype FROM okg.v_nodes ORDER BY node_id",
        )
        lexical = search(conn, session, "pinned", method="lexical")
        viz_node = viz_reads.fetch_node(conn, session, "mcp:1")
        viz_counts = {
            "nodes": viz_reads.count_nodes(conn, session),
            "edges": viz_reads.count_edges(conn, session),
        }

    assert payload["columns"] == ["node_id", "subtype"]
    assert payload["rows"] == [["mcp:1", "document"]]
    assert lexical["results"][0]["node_id"] == "mcp:1"
    assert viz_node is not None
    assert viz_node.id == "mcp:1"
    assert viz_counts == {"nodes": 1, "edges": 0}


def test_mcp_session_pin_stays_on_original_chronos_generation_after_later_publish(
    clean_runtime: str,
) -> None:
    _reset_chronos_graph(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts([
            NodeFact(
                "pin:one",
                "document",
                {"text": "first publish"},
                {"id": "pin:one"},
                {"v": 1},
            ),
        ])
    first = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert first["status"] == "published", first

    with psycopg.connect(clean_runtime) as conn:
        pinned = open_session(conn, deployment_name="default")
    assert pinned.generation_id == first["generation_id"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts([
            NodeFact(
                "pin:two",
                "document",
                {"text": "second publish"},
                {"id": "pin:two"},
                {"v": 1},
            ),
        ])
    second = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert second["status"] == "published", second
    assert second["generation_id"] != first["generation_id"]

    sql = "SELECT node_id FROM okg.v_nodes ORDER BY node_id"
    with psycopg.connect(clean_runtime) as conn:
        stale_payload = run_sql(conn, pinned, sql)
        latest_session = open_session(conn, deployment_name="default")
        latest_payload = run_sql(conn, latest_session, sql)

    assert stale_payload["pinned_generation_id"] == first["generation_id"]
    assert stale_payload["rows"] == [["pin:one"]]
    assert latest_session.generation_id == second["generation_id"]
    assert latest_payload["pinned_generation_id"] == second["generation_id"]
    assert latest_payload["rows"] == [["pin:one"], ["pin:two"]]


def test_chronos_batched_upsert_bulk_insert_and_replace(
    chronos_graph_dsn: str,
) -> None:
    """The interval backend's batched ``upsert_rows`` writes a large multi-chunk
    batch via bulk INSERT and replaces existing keys in place, with exactly one
    visible row per key — matching per-row ``upsert_row(allow_replace=True)``
    semantics. Exercises the chunk-boundary (1500 > _UPSERT_BATCH_CHUNK) and the
    split between brand-new keys (bulk insert) and existing keys (splice).
    It also exercises multi-chunk batched deletes, which are the same interval
    splice operation with a tombstone row instead of a replacement row."""
    _reset_chronos_graph(chronos_graph_dsn)
    store = ChronosGraphStore.connect(chronos_graph_dsn)
    try:
        # Batch 1: 1500 brand-new nodes -> crosses the 1000-row chunk boundary,
        # exercising the chunked existence probe + chunked bulk INSERT.
        store.apply_facts([
            NodeFact(
                node_id=f"n:{i}", subtype="page", attrs={"v": i},
                source_record_id={"id": str(i)}, source_revision={"rev": 1},
            )
            for i in range(1500)
        ])
        assert store.query("SELECT count(*) AS c FROM okg.graph_nodes")[0]["c"] == 1500
        # a value past the chunk boundary landed correctly
        assert store.query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'n:1234'"
        )[0]["attrs"] == {"v": 1234}

        # Batch 2 (one flush): replace 500 existing keys (new attrs -> splice) +
        # add 500 brand-new keys (bulk insert).
        store.apply_facts(
            [
                NodeFact(
                    node_id=f"n:{i}", subtype="page", attrs={"v": i, "gen": 2},
                    source_record_id={"id": str(i)}, source_revision={"rev": 2},
                )
                for i in range(500)
            ]
            + [
                NodeFact(
                    node_id=f"n:{i}", subtype="page", attrs={"v": i},
                    source_record_id={"id": str(i)}, source_revision={"rev": 1},
                )
                for i in range(1500, 2000)
            ]
        )
        # 2000 distinct keys, exactly one visible row each (no interval leak)
        assert store.query("SELECT count(*) AS c FROM okg.graph_nodes")[0]["c"] == 2000
        assert store.query(
            "SELECT node_id FROM okg.graph_nodes "
            "GROUP BY node_id HAVING count(*) > 1"
        ) == []
        # replaced key carries the new attrs; untouched key keeps the old;
        # brand-new key from batch 2 is present
        assert store.query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'n:0'"
        )[0]["attrs"] == {"v": 0, "gen": 2}
        assert store.query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'n:900'"
        )[0]["attrs"] == {"v": 900}
        assert store.query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'n:1750'"
        )[0]["attrs"] == {"v": 1750}

        # Batch 3: delete enough existing keys to cross the chunk boundary.
        store.apply_facts([
            NodeFact(
                node_id=f"n:{i}", subtype="page", attrs={},
                source_record_id={"id": str(i)}, source_revision={"rev": 3},
                op="R",
            )
            for i in range(1200)
        ])
        assert store.query("SELECT count(*) AS c FROM okg.graph_nodes")[0]["c"] == 800
        assert store.query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id = 'n:0'"
        ) == []
        assert store.query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'n:1750'"
        )[0]["attrs"] == {"v": 1750}
    finally:
        store.close()


def test_identical_upserts_do_not_add_interval_versions(
    clean_runtime: str,
) -> None:
    """The interval backend is the final idempotence boundary, including for
    callers that bypass ChronosGraphStore's semantic filter."""
    from okg.substrate.graph_store_internal.common import NODE_TABLE

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts(
            [
                NodeFact(
                    "noop:1",
                    "document",
                    {"text": "stable", "coordinates": (1, 2)},
                    {"id": "noop:1"},
                    {"content": "stable"},
                ),
                NodeFact(
                    "noop:2",
                    "document",
                    {"text": "stable-two"},
                    {"id": "noop:2"},
                    {"content": "stable-two"},
                ),
                NodeFact(
                    "noop:boolean",
                    "document",
                    {"flag": True},
                    {"id": "noop:boolean"},
                    {"content": "boolean"},
                ),
            ],
            source="fixture.git-history",
            run_id="first-run",
        )
        store.checkpoint_generation(1)

        session = store.checkout()
        backend = session._context._backend
        meta = backend._meta_for_ref(session._ref, NODE_TABLE)
        visible = session.query(
            "SELECT * FROM okg.graph_nodes ORDER BY node_id"
        )
        rows = [
            {column: item.get(column) for column in meta.columns}
            for item in visible
        ]
        row, row_two, row_bool = rows

        def physical_count() -> int:
            result = store.context.db.execute(
                f'SELECT count(*) AS n FROM "{meta.physical_name}"'
            ).fetchone()
            return int(result["n"])

        initial_count = physical_count()

        # JSONB normalizes a Python tuple to an array. That wire-level shape
        # difference is not a semantic change and must not add an interval.
        tuple_equivalent = deepcopy(row)
        tuple_equivalent["attrs"]["coordinates"] = (1, 2)
        session.upsert_rows(NODE_TABLE, [tuple_equivalent])
        assert physical_count() == initial_count

        # Singular backend API (not exposed by BranchSession).
        with session.transaction():
            backend.upsert_row(session.branch_id, NODE_TABLE, dict(row))
        store.checkpoint_generation(2)
        assert physical_count() == initial_count

        session = store.checkout()
        session.upsert_rows(NODE_TABLE, [dict(row), dict(row_two)])
        store.checkpoint_generation(3)
        assert physical_count() == initial_count

        session = store.checkout()
        session.apply_table_edits(
            NODE_TABLE,
            upserts=[dict(row), dict(row_two)],
            deletes=[],
        )
        store.checkpoint_generation(4)
        assert physical_count() == initial_count

        # An identical inherited-row upsert on a fork is also a no-op.
        store.create_branch("noop-fork", from_branch="default")
        count_after_fork = physical_count()
        store.checkout("noop-fork").upsert_rows(NODE_TABLE, [dict(row)])
        store.checkpoint_generation(40, branch="noop-fork")
        assert physical_count() == count_after_fork

        # One source-revision-only change in a mixed unchanged/changed batch
        # creates exactly one interval. The unchanged sibling stays suppressed.
        changed_revision = dict(row_two)
        changed_revision["source_revision"] = {"content": "revision-only"}
        store.checkout().upsert_rows(
            NODE_TABLE,
            [dict(row), changed_revision],
        )
        store.checkpoint_generation(5)
        assert physical_count() == initial_count + 1

        # A later payload-only change creates one more interval on default,
        # while the fork and generation-four pin keep the old value.
        changed = dict(row)
        changed["attrs"] = {**dict(row["attrs"]), "text": "changed"}
        store.checkout().upsert_rows(NODE_TABLE, [changed])
        store.checkpoint_generation(6)

        assert physical_count() == initial_count + 2
        assert store.checkout_generation(4).query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'noop:1'"
        )[0]["attrs"]["text"] == "stable"
        assert store.checkout_generation(6).query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'noop:1'"
        )[0]["attrs"]["text"] == "changed"
        assert store.query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'noop:1'",
            branch="noop-fork",
        )[0]["attrs"]["text"] == "stable"

        store.checkout().apply_table_edits(
            NODE_TABLE,
            upserts=[],
            deletes=[{"node_id": "noop:2"}],
        )
        store.checkpoint_generation(7)
        assert physical_count() == initial_count + 3
        assert store.checkout_generation(7).query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id = 'noop:2'"
        ) == []

        # Python bool is an int subclass, but JSONB true and 1 are distinct.
        # The exact-visible guard must retain the real scalar-type change.
        changed_bool = deepcopy(row_bool)
        changed_bool["attrs"] = {**dict(row_bool["attrs"]), "flag": 1}
        store.checkout().upsert_rows(NODE_TABLE, [changed_bool])
        store.checkpoint_generation(8)
        assert physical_count() == initial_count + 4
        stored_flag = store.checkout_generation(8).query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'noop:boolean'"
        )[0]["attrs"]["flag"]
        assert stored_flag == 1
        assert not isinstance(stored_flag, bool)
    finally:
        store.close()


def test_concurrent_identical_changed_upserts_add_one_interval_version(
    clean_runtime: str,
) -> None:
    from okg.substrate.graph_store_internal.common import NODE_TABLE

    store = ChronosGraphStore.connect(clean_runtime)
    workers: list[ChronosGraphStore] = []
    try:
        store.apply_facts(
            [
                NodeFact(
                    "concurrent:1",
                    "document",
                    {"text": "before"},
                    {"id": "concurrent:1"},
                    {"content": "before"},
                )
            ],
            source="fixture.git-history",
            run_id="initial",
        )
        store.checkpoint_generation(1)
        session = store.checkout()
        backend = session._context._backend
        meta = backend._meta_for_ref(session._ref, NODE_TABLE)
        row = session.query(
            "SELECT * FROM okg.graph_nodes WHERE node_id = 'concurrent:1'"
        )[0]
        changed = {column: row.get(column) for column in meta.columns}
        changed["attrs"] = {**dict(changed["attrs"]), "text": "after"}
        changed["source_revision"] = {"content": "after"}

        def physical_count() -> int:
            result = store.context.db.execute(
                f'SELECT count(*) AS n FROM "{meta.physical_name}"'
            ).fetchone()
            return int(result["n"])

        before = physical_count()
        barrier = threading.Barrier(2)

        def write_once() -> None:
            worker = ChronosGraphStore.connect(clean_runtime)
            workers.append(worker)
            worker_session = worker.checkout()
            barrier.wait()
            worker_session.upsert_rows(NODE_TABLE, [deepcopy(changed)])

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(write_once) for _ in range(2)]
            for future in futures:
                future.result(timeout=20)

        assert physical_count() == before + 1
        assert store.query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'concurrent:1'"
        )[0]["attrs"]["text"] == "after"
    finally:
        for worker in workers:
            worker.close()
        store.close()


def test_substitute_node_preserves_concurrent_authoritative_owner(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = ChronosGraphStore.connect(clean_runtime)
    workers: list[ChronosGraphStore] = []
    barrier = threading.Barrier(2)
    git_files_committed = threading.Event()
    thread_state = threading.local()
    prior_read_counts: dict[str, int] = {}
    prior_read_counts_lock = threading.Lock()
    original_prior = ChronosGraphStore._prior_nodes_by_id

    def synchronized_first_prior(session, node_ids):
        prior = original_prior(session, node_ids)
        source = str(getattr(thread_state, "source", "unknown"))
        with prior_read_counts_lock:
            prior_read_counts[source] = prior_read_counts.get(source, 0) + 1
        if not getattr(thread_state, "read_once", False):
            thread_state.read_once = True
            barrier.wait()
            if source == "demo.git-history":
                assert git_files_committed.wait(timeout=20)
        return prior

    monkeypatch.setattr(
        ChronosGraphStore,
        "_prior_nodes_by_id",
        staticmethod(synchronized_first_prior),
    )

    history = NodeFact(
        "file:demo:shared.py",
        "source_file",
        {
            "label": "shared.py",
            "path": "shared.py",
            "repo": "demo",
            "current_in_worktree": False,
            "source_provider": "git_history",
        },
        {"repo": "demo", "node_id": "file:demo:shared.py"},
        {"content_sha256": "history"},
        ownership_mode="substitute",
    )
    git_files = NodeFact(
        "file:demo:shared.py",
        "source_file",
        {
            "label": "shared.py",
            "path": "shared.py",
            "repo": "demo",
            "git_blob_sha": "abc123",
            "size": 42,
            "source_provider": "git_files",
        },
        {"repo": "demo", "path": "shared.py"},
        {"git_blob_sha": "abc123"},
    )

    def apply(fact: NodeFact, source: str) -> None:
        thread_state.source = source
        worker = ChronosGraphStore.connect(clean_runtime)
        workers.append(worker)
        worker.apply_facts(
            [fact],
            source=source,
            run_id=source,
            strict_endpoints=False,
        )
        if source == "demo.git-files":
            git_files_committed.set()

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(apply, history, "demo.git-history"),
                pool.submit(apply, git_files, "demo.git-files"),
            ]
            for future in futures:
                future.result(timeout=20)

        row = store.query(
            "SELECT source, attrs FROM okg.graph_nodes "
            "WHERE node_id = 'file:demo:shared.py'"
        )[0]
        assert row["source"] == "demo.git-files"
        assert row["attrs"]["git_blob_sha"] == "abc123"
        assert row["attrs"]["source_provider"] == "git_files"
        assert prior_read_counts["demo.git-history"] >= 2
    finally:
        for worker in workers:
            worker.close()
        store.close()


# Issue #1901 (#2574 slice 2): a substitute node that several sources declare
# belongs to one of them. Retracting it used to delete the row even while
# another source still declared it; the row now passes to a surviving
# declarer and is deleted only when none remains.
_SHARED_AUTHOR_ID = "author:x@example.org"


def _shared_author(source: str, *, op: str = "I") -> NodeFact:
    return NodeFact(
        _SHARED_AUTHOR_ID,
        "git_author",
        {} if op == "R" else {
            "label": "x@example.org",
            "email": "x@example.org",
            "seen_in": source,
        },
        {"author": "x@example.org", "repo": source},
        {"commit": f"{source}-head"},
        op=op,
        ownership_mode="substitute",
    )


def _shared_author_row(
    store: ChronosGraphStore, *, branch: str = "default",
) -> dict | None:
    rows = store.query(
        "SELECT source, attrs, source_record_id FROM okg.graph_nodes "
        "WHERE node_id = :node_id",
        {"node_id": _SHARED_AUTHOR_ID},
        branch=branch,
    )
    return dict(rows[0]) if rows else None


def _declaring_sources(
    store: ChronosGraphStore, *, branch: str = "default",
) -> list[str]:
    return sorted(
        str(row["source"])
        for row in store.query(
            "SELECT source FROM okg.graph_node_declarers "
            "WHERE node_id = :node_id",
            {"node_id": _SHARED_AUTHOR_ID},
            branch=branch,
        )
    )


def _apply_as(
    store: ChronosGraphStore,
    source: str,
    fact: NodeFact,
    *,
    branch: str = "default",
):
    return store.apply_facts(
        [fact], source=source, run_id=source, strict_endpoints=False,
        branch=branch,
    )


def test_shared_substitute_node_passes_to_surviving_declarer_on_retraction(
    clean_runtime: str,
) -> None:
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _apply_as(store, "repo.alpha", _shared_author("repo.alpha"))
        # beta cannot take alpha's row (substitute rule), so the row stays
        # alpha's and beta is recorded as a second declarer.
        _apply_as(store, "repo.beta", _shared_author("repo.beta"))
        row = _shared_author_row(store)
        assert row is not None and row["source"] == "repo.alpha"
        assert _declaring_sources(store) == ["repo.alpha", "repo.beta"]

        result = _apply_as(
            store, "repo.alpha", _shared_author("repo.alpha", op="R"),
        )

        row = _shared_author_row(store)
        assert row is not None, "retraction deleted a node beta still declares"
        assert row["source"] == "repo.beta"
        assert row["attrs"]["seen_in"] == "repo.beta"
        assert row["source_record_id"] == {
            "author": "x@example.org", "repo": "repo.beta",
        }
        assert row["attrs"][LINEAGE_ATTR]["source_ids"] == ["repo.beta"]
        assert result.nodes_deleted == 0
        assert result.nodes_upserted == 1
        assert result.delta_counts["nodes_retracted"] == 0
        assert result.timings_ms["transaction.node_declarers.handoffs"] == 1.0
        assert _declaring_sources(store) == ["repo.beta"]

        # The last declarer's retraction deletes the node as before.
        result = _apply_as(
            store, "repo.beta", _shared_author("repo.beta", op="R"),
        )
        assert _shared_author_row(store) is None
        assert result.nodes_deleted == 1
        assert _declaring_sources(store) == []
    finally:
        store.close()


def test_shared_substitute_node_retraction_by_non_owner_only_withdraws_its_declaration(
    clean_runtime: str,
) -> None:
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _apply_as(store, "repo.alpha", _shared_author("repo.alpha"))
        _apply_as(store, "repo.beta", _shared_author("repo.beta"))

        result = _apply_as(
            store, "repo.beta", _shared_author("repo.beta", op="R"),
        )
        row = _shared_author_row(store)
        assert row is not None and row["source"] == "repo.alpha"
        assert row["attrs"]["seen_in"] == "repo.alpha"
        assert result.nodes_deleted == 0
        assert _declaring_sources(store) == ["repo.alpha"]

        # beta withdrew, so alpha is the last declarer: its retraction deletes
        # the node rather than handing it back to beta.
        result = _apply_as(
            store, "repo.alpha", _shared_author("repo.alpha", op="R"),
        )
        assert _shared_author_row(store) is None
        assert result.nodes_deleted == 1
        assert _declaring_sources(store) == []
    finally:
        store.close()


def test_shared_substitute_node_passes_along_three_declarers_and_back(
    clean_runtime: str,
) -> None:
    """A declarer that returns after a hand-off is recorded, not the owner."""
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        for source in ("repo.alpha", "repo.gamma", "repo.beta"):
            _apply_as(store, source, _shared_author(source))
        _apply_as(store, "repo.alpha", _shared_author("repo.alpha", op="R"))
        assert _shared_author_row(store)["source"] == "repo.beta"

        # alpha declares the node again: beta keeps it; alpha is recorded.
        _apply_as(store, "repo.alpha", _shared_author("repo.alpha"))
        assert _shared_author_row(store)["source"] == "repo.beta"
        assert _declaring_sources(store) == [
            "repo.alpha", "repo.beta", "repo.gamma",
        ]

        _apply_as(store, "repo.beta", _shared_author("repo.beta", op="R"))
        row = _shared_author_row(store)
        assert row["source"] == "repo.alpha"
        assert row["attrs"]["seen_in"] == "repo.alpha"

        _apply_as(store, "repo.alpha", _shared_author("repo.alpha", op="R"))
        assert _shared_author_row(store)["source"] == "repo.gamma"
        _apply_as(store, "repo.gamma", _shared_author("repo.gamma", op="R"))
        assert _shared_author_row(store) is None
        assert _declaring_sources(store) == []
    finally:
        store.close()


def test_unchanged_substitute_redeclaration_writes_no_declarer_version(
    clean_runtime: str,
) -> None:
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _apply_as(store, "repo.alpha", _shared_author("repo.alpha"))
        first = _apply_as(store, "repo.beta", _shared_author("repo.beta"))
        assert first.timings_ms["transaction.node_declarers.recorded"] == 1.0
        replay = _apply_as(store, "repo.beta", _shared_author("repo.beta"))
        assert replay.timings_ms["transaction.node_declarers.recorded"] == 0.0
    finally:
        store.close()


def test_shared_node_declarers_follow_the_branch(clean_runtime: str) -> None:
    """Declarers branch with the graph rows, so a fork hands off too."""
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _apply_as(store, "repo.alpha", _shared_author("repo.alpha"))
        _apply_as(store, "repo.beta", _shared_author("repo.beta"))
        store.create_branch("declarer-fork", from_branch="default")
        assert _declaring_sources(store, branch="declarer-fork") == [
            "repo.alpha", "repo.beta",
        ]

        _apply_as(
            store, "repo.alpha", _shared_author("repo.alpha", op="R"),
            branch="declarer-fork",
        )
        fork_row = _shared_author_row(store, branch="declarer-fork")
        assert fork_row is not None and fork_row["source"] == "repo.beta"
        # The fork's retraction and hand-off do not reach the parent branch.
        assert _shared_author_row(store)["source"] == "repo.alpha"
        assert _declaring_sources(store) == ["repo.alpha", "repo.beta"]
        assert _declaring_sources(store, branch="declarer-fork") == [
            "repo.beta",
        ]
    finally:
        store.close()


def _race_first_prior_reads(
    monkeypatch: pytest.MonkeyPatch,
    *,
    waits_for: dict[str, str],
    committed: dict[str, threading.Event],
):
    """Make both writers read their prior rows before either takes the fence.

    The writer whose source is a key of ``waits_for`` then waits until the
    named writer has committed, fixing the commit order.
    """
    barrier = threading.Barrier(2)
    thread_state = threading.local()
    original_prior = ChronosGraphStore._prior_nodes_by_id

    def synchronized_first_prior(session, node_ids):
        prior = original_prior(session, node_ids)
        if not getattr(thread_state, "read_once", False):
            thread_state.read_once = True
            barrier.wait(timeout=20)
            other = waits_for.get(thread_state.source)
            if other is not None:
                assert committed[other].wait(timeout=20)
        return prior

    monkeypatch.setattr(
        ChronosGraphStore,
        "_prior_nodes_by_id",
        staticmethod(synchronized_first_prior),
    )
    return thread_state


def _run_racing_writes(
    clean_runtime: str,
    thread_state,
    committed: dict[str, threading.Event],
    writes: list[tuple[NodeFact, str]],
) -> None:
    workers: list[ChronosGraphStore] = []

    def apply(fact: NodeFact, source: str) -> None:
        thread_state.source = source
        worker = ChronosGraphStore.connect(clean_runtime)
        workers.append(worker)
        try:
            _apply_as(worker, source, fact)
        finally:
            committed[source].set()

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(apply, fact, source) for fact, source in writes
            ]
            for future in futures:
                future.result(timeout=40)
    finally:
        for worker in workers:
            worker.close()


@pytest.mark.parametrize("first_commit", ["retraction", "declaration"])
def test_shared_substitute_node_owner_retraction_racing_new_declaration_keeps_node(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    first_commit: str,
) -> None:
    """With ``declaration`` first, alpha read the graph before beta declared:
    a declarer lookup made before the fence would miss beta and delete."""
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _apply_as(store, "repo.alpha", _shared_author("repo.alpha"))
        committed = {
            "repo.alpha": threading.Event(),
            "repo.beta": threading.Event(),
        }
        thread_state = _race_first_prior_reads(
            monkeypatch,
            waits_for=(
                {"repo.beta": "repo.alpha"}
                if first_commit == "retraction"
                else {"repo.alpha": "repo.beta"}
            ),
            committed=committed,
        )
        _run_racing_writes(
            clean_runtime,
            thread_state,
            committed,
            [
                (_shared_author("repo.alpha", op="R"), "repo.alpha"),
                (_shared_author("repo.beta"), "repo.beta"),
            ],
        )

        row = _shared_author_row(store)
        assert row is not None, (
            f"{first_commit} committed first and the node was deleted"
        )
        assert row["source"] == "repo.beta"
        assert row["attrs"]["seen_in"] == "repo.beta"
        assert _declaring_sources(store) == ["repo.beta"]
    finally:
        store.close()


@pytest.mark.parametrize("first_commit", ["repo.alpha", "repo.beta"])
def test_shared_substitute_node_two_racing_retractions_delete_it(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    first_commit: str,
) -> None:
    """Both declarers leave at once; the node must not survive either order.

    With alpha first, beta read the row as alpha's before the fence, but the
    hand-off made it beta's: only a decision taken after the fence sees that
    beta is now the owner and the last declarer.
    """
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _apply_as(store, "repo.alpha", _shared_author("repo.alpha"))
        _apply_as(store, "repo.beta", _shared_author("repo.beta"))
        other = "repo.beta" if first_commit == "repo.alpha" else "repo.alpha"
        committed = {
            "repo.alpha": threading.Event(),
            "repo.beta": threading.Event(),
        }
        thread_state = _race_first_prior_reads(
            monkeypatch,
            waits_for={other: first_commit},
            committed=committed,
        )
        _run_racing_writes(
            clean_runtime,
            thread_state,
            committed,
            [
                (_shared_author("repo.alpha", op="R"), "repo.alpha"),
                (_shared_author("repo.beta", op="R"), "repo.beta"),
            ],
        )

        assert _shared_author_row(store) is None, (
            f"{first_commit} retracted first and the node survived"
        )
        assert _declaring_sources(store) == []
    finally:
        store.close()


def test_unregistered_declarer_table_is_refused_inside_an_open_transaction(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The writer never commits a caller's transaction to register the table."""
    from okg.substrate.graph_store_internal.node_declarers import (
        NODE_DECLARERS_TABLE,
    )

    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn, autocommit=False)
        backend = store.context._backend
        ensured: list[bool] = []
        monkeypatch.setattr(backend, "refresh_registries", lambda: None)
        monkeypatch.setattr(
            store, "ensure_node_declarers_table", lambda: ensured.append(True),
        )
        backend.tables.pop(NODE_DECLARERS_TABLE, None)
        conn.execute("SELECT 1")
        assert store.context.db.in_transaction
        with pytest.raises(SubstrateError, match="not registered"):
            _apply_as(store, "repo.alpha", _shared_author("repo.alpha"))
        assert ensured == []
        conn.rollback()


def test_unregistered_declarer_table_is_registered_on_an_idle_connection(
    clean_runtime: str,
) -> None:
    """A deployment that has not migrated gets the table on first write."""
    from okg.substrate.graph_store_internal.node_declarers import (
        NODE_DECLARERS_TABLE,
    )

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.execute(
                "DELETE FROM _chronos_branch_tables WHERE table_name = %s",
                (NODE_DECLARERS_TABLE,),
            )
        from chronos_core.branching._common import clear_all_registry_caches

        clear_all_registry_caches()
        store.context._backend.refresh_registries()
        assert NODE_DECLARERS_TABLE not in store.context._backend.tables
        store._node_declarers_registered = False

        _apply_as(store, "repo.alpha", _shared_author("repo.alpha"))
        _apply_as(store, "repo.beta", _shared_author("repo.beta"))
        assert NODE_DECLARERS_TABLE in store.context._backend.tables
        assert _declaring_sources(store) == ["repo.alpha", "repo.beta"]
    finally:
        store.close()


@pytest.mark.parametrize("first_writer", ["repo.alpha", "repo.beta"])
def test_shared_node_decision_waits_on_the_branch_fence(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    first_writer: str,
) -> None:
    """A true lock race: the second writer's transaction is open and blocked
    on ``_lock_branch_write`` while the first holds it.

    alpha (owner) retracts; beta newly declares. Whichever holds the fence
    first, the node ends present and owned by beta.
    """
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _apply_as(store, "repo.alpha", _shared_author("repo.alpha"))
        second_writer = (
            "repo.beta" if first_writer == "repo.alpha" else "repo.alpha"
        )
        holding = threading.Event()
        release = threading.Event()
        thread_state = threading.local()
        original_lock = ChronosGraphStore._lock_branch_write

        def lock_then_hold(self, branch):
            original_lock(self, branch)
            if getattr(thread_state, "source", None) == first_writer:
                holding.set()
                assert release.wait(timeout=30)

        monkeypatch.setattr(
            ChronosGraphStore, "_lock_branch_write", lock_then_hold,
        )
        facts = {
            "repo.alpha": _shared_author("repo.alpha", op="R"),
            "repo.beta": _shared_author("repo.beta"),
        }
        workers: list[ChronosGraphStore] = []

        def apply(source: str) -> None:
            thread_state.source = source
            worker = ChronosGraphStore.connect(clean_runtime)
            workers.append(worker)
            _apply_as(worker, source, facts[source])

        def advisory_waiters() -> int:
            with psycopg.connect(clean_runtime, autocommit=True) as conn:
                return int(conn.execute(
                    "SELECT count(*) FROM pg_locks "
                    "WHERE locktype = 'advisory' AND NOT granted"
                ).fetchone()[0])

        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(apply, first_writer)
                assert holding.wait(timeout=30)
                second = pool.submit(apply, second_writer)
                deadline = time.monotonic() + 20
                while advisory_waiters() == 0:
                    assert time.monotonic() < deadline, (
                        "the second writer never blocked on the branch fence"
                    )
                    time.sleep(0.05)
                assert not second.done()
                release.set()
                first.result(timeout=30)
                second.result(timeout=30)
        finally:
            release.set()
            for worker in workers:
                worker.close()

        row = _shared_author_row(store)
        assert row is not None, f"{first_writer} held the fence; node deleted"
        assert row["source"] == "repo.beta"
        assert _declaring_sources(store) == ["repo.beta"]
    finally:
        store.close()


def test_handoff_of_a_declaration_under_another_subtype_deletes_instead(
    clean_runtime: str,
) -> None:
    """A stored copy whose subtype differs from the live row is not written:
    it may name a subtype the catalog no longer admits."""
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _apply_as(store, "repo.alpha", _shared_author("repo.alpha"))
        _apply_as(
            store,
            "repo.beta",
            NodeFact(
                _SHARED_AUTHOR_ID,
                "legacy_author",
                {"email": "x@example.org", "seen_in": "repo.beta"},
                {"author": "x@example.org", "repo": "repo.beta"},
                {"commit": "repo.beta-head"},
                ownership_mode="substitute",
            ),
        )
        result = _apply_as(
            store, "repo.alpha", _shared_author("repo.alpha", op="R"),
        )
        assert _shared_author_row(store) is None
        assert result.nodes_deleted == 1
        assert result.timings_ms[
            "transaction.node_declarers.handoffs_refused_subtype"
        ] == 1.0
    finally:
        store.close()


def test_handoff_skips_a_declarer_under_another_subtype_for_one_that_matches(
    clean_runtime: str,
) -> None:
    """Three declarers: the lowest-named one's stored copy has a different
    subtype, the next matches the live row. The owner's retraction passes the
    node to the matching one rather than deleting it."""
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _apply_as(store, "repo.alpha", _shared_author("repo.alpha"))
        _apply_as(
            store,
            "repo.beta",
            NodeFact(
                _SHARED_AUTHOR_ID,
                "legacy_author",
                {"email": "x@example.org", "seen_in": "repo.beta"},
                {"author": "x@example.org", "repo": "repo.beta"},
                {"commit": "repo.beta-head"},
                ownership_mode="substitute",
            ),
        )
        _apply_as(store, "repo.gamma", _shared_author("repo.gamma"))

        result = _apply_as(
            store, "repo.alpha", _shared_author("repo.alpha", op="R"),
        )
        row = _shared_author_row(store)
        assert row is not None, (
            "a non-matching lowest-named declarer caused a delete"
        )
        assert row["source"] == "repo.gamma"
        assert result.nodes_deleted == 0
        assert result.timings_ms[
            "transaction.node_declarers.handoffs_refused_subtype"
        ] == 0.0
    finally:
        store.close()


def test_handoff_restamps_searchable_text_like_a_normal_write(
    clean_runtime: str,
) -> None:
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _apply_as(store, "repo.alpha", _shared_author("repo.alpha"))
        _apply_as(store, "repo.beta", _shared_author("repo.beta"))
        _apply_as(store, "repo.alpha", _shared_author("repo.alpha", op="R"))
        handed = store.query(
            "SELECT attrs, text_hash FROM okg.graph_nodes WHERE node_id = :n",
            {"n": _SHARED_AUTHOR_ID},
        )[0]
        _apply_as(store, "repo.beta", _shared_author("repo.beta", op="R"))
        _apply_as(store, "repo.beta", _shared_author("repo.beta"))
        direct = store.query(
            "SELECT attrs, text_hash FROM okg.graph_nodes WHERE node_id = :n",
            {"n": _SHARED_AUTHOR_ID},
        )[0]
        assert handed["text_hash"] == direct["text_hash"]
        assert handed["attrs"].get("text") == direct["attrs"].get("text")
    finally:
        store.close()


def test_compensating_a_failed_run_withdraws_its_declarations(
    clean_runtime: str,
) -> None:
    """A failed run's declaration of a node it did not own has no graph row to
    retract; compensation must delete it, or a later hand-off would write the
    failed run's content."""
    from okg.substrate.ingest.compensation import retract_chronos_run

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _apply_as(store, "repo.alpha", _shared_author("repo.alpha"))
        store.apply_facts(
            [_shared_author("repo.gamma")],
            source="repo.gamma",
            run_id="run-gamma-failed",
            strict_endpoints=False,
        )
        assert _declaring_sources(store) == ["repo.alpha", "repo.gamma"]

        retract_chronos_run(
            clean_runtime,
            admission_id=None,
            source="repo.gamma",
            run_id="run-gamma-failed",
        )
        assert _declaring_sources(store) == ["repo.alpha"]

        _apply_as(store, "repo.alpha", _shared_author("repo.alpha", op="R"))
        assert _shared_author_row(store) is None, (
            "the failed run's declaration inherited the node"
        )
    finally:
        store.close()


def test_rejecting_a_quarantined_run_withdraws_its_declarations(
    clean_runtime: str,
) -> None:
    import uuid

    from okg.substrate.admission import QUARANTINE_BRANCH
    from okg.substrate.runtime.admission_workflows import (
        _drop_quarantined_source_run,
    )

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _apply_as(store, "repo.alpha", _shared_author("repo.alpha"))
        store.create_branch(QUARANTINE_BRANCH, from_branch="default")
        store.apply_facts(
            [_shared_author("repo.gamma")],
            branch=QUARANTINE_BRANCH,
            source="repo.gamma",
            run_id="run-gamma-held",
            strict_endpoints=False,
        )
        assert _declaring_sources(store, branch=QUARANTINE_BRANCH) == [
            "repo.alpha", "repo.gamma",
        ]
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            _drop_quarantined_source_run(
                conn,
                envelope={
                    "source": "repo.gamma",
                    "run_id": "run-gamma-held",
                    "admission_id": str(uuid.uuid4()),
                },
            )
        assert _declaring_sources(store, branch=QUARANTINE_BRANCH) == [
            "repo.alpha",
        ]
    finally:
        store.close()


def test_node_attrs_do_not_implicitly_change_ownership_mode(
    clean_runtime: str,
) -> None:
    """Ownership behavior comes from the contract, never source heuristics."""
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        node_id = "file:demo:explicit.py"
        store.apply_facts(
            [
                NodeFact(
                    node_id,
                    "source_file",
                    {"source_provider": "git_files", "value": "rich"},
                    {"id": node_id},
                    {"revision": "rich"},
                )
            ],
            source="owner.primary",
            run_id="primary",
        )
        store.apply_facts(
            [
                NodeFact(
                    node_id,
                    "source_file",
                    {"source_provider": "git_history", "value": "new"},
                    {"id": node_id},
                    {"revision": "new"},
                )
            ],
            source="owner.replacement",
            run_id="replacement",
        )

        row = store.query(
            "SELECT source, attrs FROM okg.graph_nodes "
            "WHERE node_id = 'file:demo:explicit.py'"
        )[0]
        assert row["source"] == "owner.replacement"
        assert row["attrs"]["value"] == "new"
    finally:
        store.close()


def test_graph_registered_probe_keeps_the_transaction_usable(
    clean_runtime: str,
) -> None:
    """A missing Chronos registry reads as "not registered" and leaves the
    caller's transaction usable. Reading the absent table and catching the
    error used to abort the transaction, so migrate's next statement failed
    with "current transaction is aborted" (the DBOS reset in
    tests/substrate/test_git_history_reemission_e2e.py)."""
    from chronos_core.branching.sql_adapters import PostgresDatabaseAdapter
    from okg.substrate.graph_store_internal.common import graph_registered

    with psycopg.connect(clean_runtime) as conn:
        conn.execute("CREATE SCHEMA probe_empty")
        conn.execute("SET LOCAL search_path TO probe_empty")
        assert graph_registered(PostgresDatabaseAdapter(conn)) is False
        assert conn.execute("SELECT 1").fetchone() == (1,)
        conn.rollback()
