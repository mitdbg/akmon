"""Regression test for `add-source-runner-dedup`.

The substrate's UNIQUE constraint on (dedupe_key, observed_at)
catches duplicate facts at COPY time, but the COPY aborts the
ENTIRE adapter run on the first violation. Pre-fix this meant
that a single sloppy emit (e.g. legacy openspec emitting
`change:<slug>` twice for the same active+archived slug) wiped
out the entire ingest.

After the fix:
  - SourceRunner dedupes in memory between materialize and
    COPY using `node_id` as the NodeFact identity and
    `(src, dst, edge_type)` as the EdgeFact identity;
  - keeps last-write-wins per identity;
  - counts the drops + first-dropped-id in `RunResult`;
  - completes the run successfully.
"""
from __future__ import annotations

import json

import psycopg
import pytest

from okg.substrate.ingest import SourceRunner
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    SourceRun,
)
from tests.substrate._db_helpers import seed_catalog_version


pytestmark = pytest.mark.postgres



class _DupNodeAdapter:
    """Adapter emitting 100 NodeFacts; 30 share node_ids with
    an earlier emit. `last_value` records the order each
    node_id is emitted last so the test can assert
    last-write-wins."""

    name = "dup-node"
    profile = "discovery_crawl"

    def __init__(self) -> None:
        # 70 distinct node_ids + 30 dups (re-emit of n:0..n:29)
        self.last_value: dict[str, str] = {}

    def run(self, run_id: str, *, mode: str = "cursor") -> SourceRun:
        facts: list[NodeFact] = []
        for i in range(70):
            v = f"v0:{i}"
            facts.append(NodeFact(
                node_id=f"n:{i}",
                subtype="concept",
                attrs={"text": v},
                source_record_id={"id": f"n:{i}"},
                source_revision={"v": 1, "rev": run_id},
                op="I",
            ))
            self.last_value[f"n:{i}"] = v
        for i in range(30):
            v = f"v1:{i}"
            facts.append(NodeFact(
                node_id=f"n:{i}",
                subtype="concept",
                attrs={"text": v},
                source_record_id={"id": f"n:{i}"},
                source_revision={"v": 2, "rev": run_id},
                op="I",
            ))
            self.last_value[f"n:{i}"] = v  # last write
        return SourceRun(
            facts=facts, completed_scope=True,
            next_cursor=None, run_mode=mode,
        )


class _DupEdgeAdapter:
    """100 EdgeFacts; 30 with duplicate (src, dst, edge_type)."""

    name = "dup-edge"
    profile = "discovery_crawl"

    def run(self, run_id: str, *, mode: str = "cursor") -> SourceRun:
        # 70 distinct (src, dst, etype) + 30 dups.
        facts: list = []
        # Emit nodes first so the projector has endpoints.
        for i in range(141):
            facts.append(NodeFact(
                node_id=f"e:n:{i}",
                subtype="concept",
                attrs={"text": f"label:{i}"},
                source_record_id={"id": f"e:n:{i}"},
                source_revision={"v": 1, "rev": run_id},
                op="I",
            ))
        for i in range(70):
            facts.append(EdgeFact(
                src=f"e:n:{2*i}",
                dst=f"e:n:{2*i+1}",
                edge_type="links_to",
                attrs={"v": "first"},
                source_record_id={"id": f"e:{i}"},
                source_revision={"v": 1, "rev": run_id},
                op="I",
            ))
        for i in range(30):
            facts.append(EdgeFact(
                src=f"e:n:{2*i}",
                dst=f"e:n:{2*i+1}",
                edge_type="links_to",
                attrs={"v": "second"},  # last-write-wins
                source_record_id={"id": f"e:{i}"},
                source_revision={"v": 2, "rev": run_id},
                op="I",
            ))
        return SourceRun(
            facts=facts, completed_scope=True,
            next_cursor=None, run_mode=mode,
        )


class _ReinsertAfterRetractionAdapter:
    name = "reinsert-after-retraction"
    profile = "discovery_crawl"

    def run(self, run_id: str, *, mode: str = "cursor") -> SourceRun:
        del run_id
        return SourceRun(
            facts=[
                NodeFact(
                    node_id="n:reinsert",
                    subtype="concept",
                    attrs={"text": "same"},
                    source_record_id={"id": "n:reinsert"},
                    source_revision={"v": "new"},
                    op="I",
                ),
                NodeFact(
                    node_id="n:dst",
                    subtype="concept",
                    attrs={"text": "dst"},
                    source_record_id={"id": "n:dst"},
                    source_revision={"v": "new"},
                    op="I",
                ),
                EdgeFact(
                    src="n:reinsert",
                    dst="n:dst",
                    edge_type="links_to",
                    attrs={"v": "same"},
                    source_record_id={"id": "e:reinsert"},
                    source_revision={"v": "new"},
                    op="I",
                ),
            ],
            completed_scope=True,
            next_cursor=None,
            run_mode=mode,
        )


class _SubtypeMigrationAdapter:
    name = "subtype-migration"
    profile = "discovery_crawl"

    def run(self, run_id: str, *, mode: str = "cursor") -> SourceRun:
        del run_id
        return SourceRun(
            facts=[
                NodeFact(
                    node_id="n:subtype",
                    subtype="new_type",
                    attrs={"text": "same"},
                    source_record_id={"id": "n:subtype"},
                    source_revision={"v": "new"},
                    op="I",
                ),
            ],
            completed_scope=True,
            next_cursor=None,
            run_mode=mode,
        )


def test_runner_dedup_node_facts(clean_runtime: str) -> None:
    """Dup adapter: 100 node emits, 30 dups. Runner dedupes in
    memory; counter reads 30; Chronos graph ends with 70 nodes."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    adapter = _DupNodeAdapter()
    result = SourceRunner(
        clean_runtime, source_name="test.dup-node",
    ).run(adapter, mode="scope_complete")

    assert result.dedupe_drops_node == 30, result
    assert result.nodes_appended == 70

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        assert store.query(
            """
            SELECT count(DISTINCT node_id) AS n
              FROM okg.graph_nodes
             WHERE source = 'test.dup-node'
            """
        ) == [{"n": 70}]

        # last-write-wins: per node_id, the latest attrs landed.
        for nid, expected in adapter.last_value.items():
            rows = store.query(
                """
                SELECT attrs
                  FROM okg.graph_nodes
                 WHERE node_id = :node_id
                """,
                {"node_id": nid},
            )
            assert rows[0]["attrs"]["text"] == expected, (nid, expected)
    finally:
        store.close()


def test_runner_dedup_edge_facts(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    adapter = _DupEdgeAdapter()
    result = SourceRunner(
        clean_runtime, source_name="test.dup-edge",
    ).run(adapter, mode="scope_complete")

    assert result.dedupe_drops_edge == 30, result
    assert result.edges_appended == 70

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        assert store.query(
            """
            SELECT count(*) AS n
              FROM okg.graph_edges
             WHERE source = 'test.dup-edge'
            """
        ) == [{"n": 70}]
        # last-write-wins on the latest fact_id row per
        # (src,dst,edge_type): attrs->>'v' should be 'second'
        # for the 30 duplicated edges, 'first' for the rest.
        rows = store.query(
            """
            SELECT attrs
              FROM okg.graph_edges
             WHERE source = 'test.dup-edge'
            """
        )
        by_value: dict[str, int] = {}
        for row in rows:
            value = row["attrs"]["v"]
            by_value[value] = by_value.get(value, 0) + 1
    finally:
        store.close()
    assert by_value.get("first") == 40
    assert by_value.get("second") == 30


def test_runner_dedup_preserves_reinsert_after_retraction(
    clean_runtime: str,
) -> None:
    source = "test.reinsert-after-retraction"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        _seed_retracted_identity(conn, source)

    result = SourceRunner(
        clean_runtime, source_name=source,
    ).run(_ReinsertAfterRetractionAdapter(), mode="scope_complete")

    assert result.nodes_appended == 2
    assert result.edges_appended == 1
    assert result.dedupe_drops_node == 0
    assert result.dedupe_drops_edge == 0
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        assert store.query(
            """
            SELECT node_id
              FROM okg.graph_nodes
             WHERE source = :source
             ORDER BY node_id
            """,
            {"source": source},
        ) == [{"node_id": "n:dst"}, {"node_id": "n:reinsert"}]
        assert store.query(
            """
            SELECT src, edge_type, dst
              FROM okg.graph_edges
             WHERE source = :source
            """,
            {"source": source},
        ) == [{
            "src": "n:reinsert",
            "edge_type": "links_to",
            "dst": "n:dst",
        }]
    finally:
        store.close()


def test_runner_dedup_preserves_subtype_migration(
    clean_runtime: str,
) -> None:
    source = "test.subtype-migration"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES ('n:subtype', 'old_type', '{"text": "same"}'::jsonb,
                        %s, '{"id": "n:subtype"}'::jsonb,
                        '{"v": "old"}'::jsonb, '2026-01-01T00:00:00Z',
                        %s, '00000000-0000-0000-0000-000000000001', 'I')
                """,
                (source, f"{source}:old"),
            )

    result = SourceRunner(
        clean_runtime, source_name=source, write_fact_log=True,
    ).run(_SubtypeMigrationAdapter(), mode="scope_complete")

    assert result.nodes_appended == 1
    assert result.dedupe_drops_node == 0
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT subtype
              FROM okg.node_facts
             WHERE source = %s AND node_id = 'n:subtype'
             ORDER BY observed_at DESC, fact_id DESC
             LIMIT 1
            """,
            (source,),
        )
        assert cur.fetchone()[0] == "new_type"


def test_runner_dedup_first_dropped_id_set(clean_runtime: str) -> None:
    """When dedup drops one or more facts, RunResult records
    the first dropped id so operators can find the offending
    legacy emit."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    result = SourceRunner(
        clean_runtime, source_name="test.dup-node-2",
    ).run(_DupNodeAdapter(), mode="scope_complete")
    assert result.dedupe_drops_node > 0
    assert result.dedupe_first_dropped_id == "n:0"


def _seed_retracted_identity(conn: psycopg.Connection, source: str) -> None:
    run_id = "00000000-0000-0000-0000-000000000001"
    with conn.cursor() as cur:
        for op, suffix, observed_at in (
            ("I", "insert", "2026-01-01T00:00:00Z"),
            ("R", "retract", "2026-01-02T00:00:00Z"),
        ):
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES ('n:reinsert', 'concept', '{"text": "same"}'::jsonb,
                        %s, '{"id": "n:reinsert"}'::jsonb,
                        %s::jsonb, %s::timestamptz, %s, %s, %s)
                """,
                (
                    source,
                    json.dumps({"v": suffix}),
                    observed_at,
                    f"{source}:node:{suffix}",
                    run_id,
                    op,
                ),
            )
            cur.execute(
                """
                INSERT INTO okg.edge_facts
                  (edge_id, src, dst, edge_type, attrs, provenance,
                   confidence, source, source_record_id, source_revision,
                   observed_at, dedupe_key, run_id, op)
                VALUES (nextval('okg.edge_facts_edge_id_seq'),
                        'n:reinsert', 'n:dst', 'links_to',
                        '{"v": "same"}'::jsonb, 'explicit', 1.0,
                        %s, '{"id": "e:reinsert"}'::jsonb,
                        %s::jsonb, %s::timestamptz, %s, %s, %s)
                """,
                (
                    source,
                    json.dumps({"v": suffix}),
                    observed_at,
                    f"{source}:edge:{suffix}",
                    run_id,
                    op,
                ),
            )
