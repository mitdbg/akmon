from __future__ import annotations

from types import SimpleNamespace
from typing import Any
import uuid

import psycopg
import pytest
from psycopg import sql

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher.publisher import (
    Publisher,
    _publish_telemetry_status_message,
)


class _Cursor:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, _query: object, _params: object = None) -> None:
        return None

    def __iter__(self):
        return iter(self.rows)


class _Connection:
    def __init__(self, batches: list[list[tuple[Any, ...]]]) -> None:
        self.batches = iter(batches)

    def cursor(self) -> _Cursor:
        return _Cursor(next(self.batches))


def test_publish_telemetry_status_message_never_returns_mapping() -> None:
    assert _publish_telemetry_status_message(None) is None
    assert _publish_telemetry_status_message("blocked") == "blocked"
    assert _publish_telemetry_status_message({
        "failed_consistency_gates": [{
            "gate_name": "transform_consistency_gate",
            "details": {"dangling_edge_ids": [1]},
        }],
    }) == "transform_consistency_gate"
    assert _publish_telemetry_status_message({}) == "publish_blocked"


def test_graph_effect_retention_interleaves_nodes_and_edges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # One segment written since the previous generation: the single-segment
    # sampling query (the segment walk itself is exercised against Postgres
    # in tests/substrate/ingest/test_run_base_undo.py).
    monkeypatch.setattr(
        Publisher, "_chronos_segments_written_since",
        staticmethod(lambda _conn, **_kwargs: [1]),
    )
    conn = _Connection([
        [
            ("okg.graph_nodes", "nodes_physical"),
            ("okg.graph_edges", "edges_physical"),
        ],
        [
            ("node:a", "added", "file", None, None),
            ("node:b", "updated", "file", None, None),
            ("node:c", "retracted", "file", None, None),
        ],
        [
            ("edge:a", "added", "contains", "node:a", "node:b"),
            ("edge:b", "updated", "contains", "node:b", "node:c"),
            ("edge:c", "retracted", "contains", "node:c", "node:a"),
        ],
    ])

    manifest = Publisher._chronos_graph_effect_manifest(
        conn,  # type: ignore[arg-type]
        branch_name="default",
        previous=SimpleNamespace(ref="previous-segment"),
        limit=4,
    )

    assert [
        (item["object_kind"], item["object_id"], item["change_kind"])
        for item in manifest["items"]
    ] == [
        ("node", "node:a", "added"),
        ("edge", "edge:a", "added"),
        ("node", "node:b", "updated"),
        ("edge", "edge:b", "updated"),
    ]


@pytest.mark.postgres
def test_graph_effect_capture_reads_real_chronos_write_boundary(clean_runtime: str) -> None:
    suffix = uuid.uuid4().hex[:8]
    original_id = f"concept:effect-original:{suffix}"
    added_id = f"concept:effect-added:{suffix}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store.apply_facts([
            NodeFact(original_id, "concept", {"text": "before"},
                     {"id": original_id}, {"v": 1}),
        ], source="graph-effect-test")
        previous_id = store.checkpoint_generation(
            f"graph-effect-before-{suffix}",
            deployment_name=f"graph-effect-{suffix}",
            catalog_version_id=1,
        )
        previous = store.get_generation(previous_id)
        store.apply_facts([
            NodeFact(original_id, "concept", {"text": "after"},
                     {"id": original_id}, {"v": 2}),
            NodeFact(added_id, "concept", {"text": "added"},
                     {"id": added_id}, {"v": 1}),
        ], source="graph-effect-test")

        manifest = Publisher._chronos_graph_effect_manifest(
            conn,
            branch_name="default",
            previous=previous,
            limit=10,
        )

    captured = {(item["object_id"], item["change_kind"]) for item in manifest["items"]}
    assert (original_id, "updated") in captured
    assert (added_id, "added") in captured


@pytest.mark.postgres
def test_graph_effect_limit_has_bounded_historical_edge_plan(
    clean_runtime: str,
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.publisher-budget "
        "test.runtime-authority.publisher-budget",
    )
    historical_rows = 50_000
    result_limit = 5
    candidate_limit = result_limit * 4
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn, ensure_schema=True)
        with conn.cursor() as cur:
            cur.execute(
                """SELECT physical_table
                     FROM _chronos_branch_tables
                    WHERE table_name = 'okg.graph_edges'
                      AND backend = 'interval'"""
            )
            physical_table = str(cur.fetchone()[0])
            cur.execute(
                """SELECT b.current_segment_id, s.live_lo, s.live_hi
                     FROM _chronos_branch_interval_branches b
                     JOIN _chronos_branch_interval_segments s
                       ON s.segment_id = b.current_segment_id
                    WHERE b.branch_id = 'default'"""
            )
            current_segment_id, live_lo, live_hi = cur.fetchone()
            historical_segment_id = int(current_segment_id) + 1_000_000
            cur.execute(
                sql.SQL(
                    """INSERT INTO {physical} (
                         edge_id, src, dst, edge_type, attrs, provenance,
                         confidence, source_record_id, source_revision,
                         live_lo, live_hi, writer_segment_id, deleted
                       )
                       SELECT n, 'historical-src-' || n, 'historical-dst-' || n,
                              'historical', '{{}}'::jsonb, 'explicit', 1.0,
                              '{{}}'::jsonb, '{{}}'::jsonb,
                              %s, %s, %s, FALSE
                         FROM generate_series(1, %s) AS n"""
                ).format(physical=sql.Identifier(physical_table)),
                (
                    int(live_lo),
                    int(live_hi),
                    historical_segment_id,
                    historical_rows,
                ),
            )
            cur.execute(
                sql.SQL(
                    """INSERT INTO {physical} (
                         edge_id, src, dst, edge_type, attrs, provenance,
                         confidence, source_record_id, source_revision,
                         live_lo, live_hi, writer_segment_id, deleted
                       )
                       VALUES (
                         %s, 'current-src', 'current-dst', 'current',
                         '{{}}'::jsonb, 'explicit', 1.0, '{{}}'::jsonb,
                         '{{}}'::jsonb, %s, %s, %s, FALSE
                       )"""
                ).format(physical=sql.Identifier(physical_table)),
                (
                    historical_rows + 1,
                    int(live_lo),
                    int(live_hi),
                    int(current_segment_id),
                ),
            )
            cur.execute(
                sql.SQL("ANALYZE {}").format(sql.Identifier(physical_table))
            )
            query = Publisher._chronos_graph_effect_identity_query(
                physical_table=physical_table,
                key_column="edge_id",
                type_column="edge_type",
                is_edge=True,
            )
            cur.execute(
                sql.SQL("EXPLAIN (ANALYZE, FORMAT JSON) {}").format(query),
                (
                    "default",
                    candidate_limit,
                    str(current_segment_id),
                    result_limit,
                ),
            )
            plan = cur.fetchone()[0][0]["Plan"]

    nodes = list(_walk_plan(plan))
    physical_scans = [
        node
        for node in nodes
        if node.get("Relation Name") == physical_table
    ]
    assert physical_scans
    assert all(node["Node Type"] != "Seq Scan" for node in physical_scans)
    assert any(
        "writer_segment_id" in str(node.get("Index Cond") or "")
        and int(node.get("Actual Rows") or 0) <= candidate_limit
        for node in nodes
    )
    assert all(
        int(node.get("Actual Rows") or 0) <= candidate_limit
        for node in nodes
        if node["Node Type"] == "Sort"
    )


def _walk_plan(plan: dict[str, Any]):
    yield plan
    for child in plan.get("Plans", []):
        yield from _walk_plan(child)
